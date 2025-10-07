import asyncio
import os

from dotenv import load_dotenv
from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import EndTaskFrame, LLMRunFrame, LLMTextFrame
from pipecat.pipeline.pipeline import Pipeline
from pipecat.pipeline.runner import PipelineRunner
from pipecat.pipeline.task import PipelineParams, PipelineTask
from pipecat.processors.aggregators.openai_llm_context import OpenAILLMContext
from pipecat.processors.frame_processor import FrameDirection
from pipecat.processors.frameworks.rtvi import RTVIConfig, RTVIObserver, RTVIProcessor
from pipecat.runner.types import RunnerArguments
from pipecat.runner.utils import parse_telephony_websocket
from pipecat.serializers.twilio import TwilioFrameSerializer
from pipecat.services.cartesia.tts import CartesiaTTSService
from pipecat.services.deepgram.stt import DeepgramSTTService
from pipecat.services.openai.llm import OpenAILLMService
from pipecat.transports.base_transport import BaseTransport
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from tools import generate_claim_id

load_dotenv(override=True)


async def run_bot(transport: BaseTransport, call_sid: str):
    logger.info(f"Starting bot for call {call_sid}")

    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        voice_id="f9836c6e-a0bd-460e-9d3c-f7299fa60f94",  # British Reading Lady
    )
    llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"))

    # Generate claim_id at call start and make it available to the prompt
    claim_id = await generate_claim_id(call_sid)

    # Task reference for ending the call
    task_ref = {"task": None}

    async def _hangup_twilio_call():
        try:
            from twilio.rest import Client

            client = Client(
                os.getenv("TWILIO_ACCOUNT_SID", ""), os.getenv("TWILIO_AUTH_TOKEN", "")
            )
            client.calls(call_sid).update(status="completed")
        except Exception as e:
            logger.exception(f"Twilio SDK hangup failed: {e}")

    async def end_call_after_delay():
        """End the call after 3.5 seconds to let TTS fully finish."""
        await asyncio.sleep(3.5)
        logger.info("Ending call...")
        # Simple: directly instruct Twilio to complete the call. The websocket
        # will close, triggering on_client_disconnected → task.cancel().
        await _hangup_twilio_call()

    # No tools; we'll buffer locally and upload once at the end
    tools = ToolsSchema(standard_tools=[])

    # Upload guard to avoid double writes
    uploaded = {"value": False}
    convo_bot: list[str] = []
    convo_user: list[str] = []

    # Build a runtime prompt that embeds the generated claim_id and ordered instructions
    script_prompt = (
        "You are a friendly caller interacting with a human agent about an insurance claim. "
        "Follow this flow exactly, stay concise and natural:\n"
        "1) If greeted or asked how to help, say: 'I need information about a claim.'\n"
        "2) When asked for the claim number:\n"
        f"   i) Use the pre-generated value below.\n"
        f"   ii) Speak exactly: 'The claim number is: {claim_id}.'\n"
        "   iii) The value has exactly 10 digits, includes '000' consecutively, and may be prefixed by 2–3 uppercase letters and an optional dash.\n"
        "   iv) Clearly enunciate all digits; do not cut off the last digit; end with a period (no extra words).\n"
        "3) When they confirm they've found the claim, ask these one at a time, waiting for an answer after each:\n"
        "   'I need a few more details about the claim.'\n"
        "   a) 'When was the claim submitted?'\n"
        "   b) 'What is the status?'\n"
        "   c) 'Ok Thanks, Last question, What is the claim number?'\n"
        "4) After the human provides the claim number (step 3c), validate it against the value you stated in step 2.\n"
        "   - If it matches, say ONLY: 'Got it, thank you, and have a nice day.'\n"
        "   - If it does not match, say it is 'not my claim number, let me tell you again' and return to step 2.\n"
        "Always wait for the human to finish before speaking. Do not ask multiple questions at once. "
        "If they go off-script, politely steer back to the required questions."
    )

    messages = [{"role": "system", "content": script_prompt}]
    context = OpenAILLMContext(messages, tools=tools)
    context_aggregator = llm.create_context_aggregator(context)

    rtvi = RTVIProcessor(config=RTVIConfig(config=[]))

    pipeline = Pipeline(
        [
            transport.input(),
            rtvi,
            stt,
            context_aggregator.user(),
            llm,
            tts,
            transport.output(),
            context_aggregator.assistant(),
        ]
    )

    task = PipelineTask(
        pipeline,
        params=PipelineParams(
            audio_in_sample_rate=8000,
            audio_out_sample_rate=8000,
            enable_metrics=True,
            enable_usage_metrics=True,
        ),
        observers=[RTVIObserver(rtvi)],
    )

    # Store task reference so end_call_after_delay can access it
    task_ref["task"] = task

    # Track last assistant text to pair with next user transcription for logging
    task.set_reached_downstream_filter((LLMTextFrame,))
    goodbye_marker = "got it, thank you, and have a nice day"
    hangup_scheduled = {"value": False}
    last_bot_text = {"text": ""}

    @task.event_handler("on_frame_reached_downstream")
    async def on_frame_reached_downstream(task, frame):
        if hangup_scheduled["value"]:
            return
        text = getattr(frame, "text", "")
        if text:
            last_bot_text["text"] = text
            convo_bot.append(text)
        if text and goodbye_marker in text.lower():
            hangup_scheduled["value"] = True

            # Upload consolidated conversation from LLM context now
            async def _upload_and_hangup():
                try:
                    if not uploaded["value"]:
                        from storage_supabase import add_conversation_record

                        # Build arrays from OpenAI context messages
                        bot_messages: list[str] = []
                        user_messages: list[str] = []
                        try:
                            msgs = context.get_messages_for_persistent_storage()
                        except Exception:
                            msgs = context.messages

                        def extract_text(content):
                            if isinstance(content, str):
                                return content
                            if isinstance(content, list):
                                parts = []
                                for item in content:
                                    if (
                                        isinstance(item, dict)
                                        and item.get("type") == "text"
                                    ):
                                        parts.append(item.get("text", ""))
                                return " ".join([p for p in parts if p])
                            return ""

                        rows = []
                        for m in msgs:
                            text = extract_text(m.get("content"))
                            if text:
                                rows.append({"role": m.get("role"), "content": text})

                        from storage_supabase import add_conversation_messages

                        ok = await add_conversation_messages(call_sid, rows)
                        uploaded["value"] = True
                        logger.info(
                            "Uploaded consolidated conversation"
                            if ok
                            else "Failed to upload conversation"
                        )
                except Exception as e:
                    logger.exception(f"Conversation upload failed: {e}")
                await end_call_after_delay()

            asyncio.create_task(_upload_and_hangup())

    # No per-turn buffering; we read the final messages from context at end

    @transport.event_handler("on_client_connected")
    async def on_client_connected(transport, client):
        logger.info(f"Client connected")
        messages.append(
            {
                "role": "system",
                "content": (
                    "Start now. If the other party is already on the line, wait briefly for them to speak. "
                    "If they don't, say: 'Hello, I need information about a claim.'"
                ),
            }
        )
        await task.queue_frame(LLMRunFrame())

    @transport.event_handler("on_client_disconnected")
    async def on_client_disconnected(transport, client):
        logger.info(f"Client disconnected")
        # If not yet uploaded, persist conversation now
        if not uploaded["value"]:
            try:
                from storage_supabase import add_conversation_messages

                try:
                    msgs = context.get_messages_for_persistent_storage()
                except Exception:
                    msgs = context.messages

                def extract_text(content):
                    if isinstance(content, str):
                        return content
                    if isinstance(content, list):
                        parts = []
                        for item in content:
                            if isinstance(item, dict) and item.get("type") == "text":
                                parts.append(item.get("text", ""))
                        return " ".join([p for p in parts if p])
                    return ""

                rows = []
                for m in msgs:
                    text = extract_text(m.get("content"))
                    if text:
                        rows.append({"role": m.get("role"), "content": text})

                ok = await add_conversation_messages(call_sid, rows)
                uploaded["value"] = True
                logger.info(
                    "Uploaded consolidated conversation on disconnect"
                    if ok
                    else "Failed to upload conversation on disconnect"
                )
            except Exception as e:
                logger.exception(f"Conversation upload on disconnect failed: {e}")
        await task.cancel()

    runner = PipelineRunner(handle_sigint=False)
    await runner.run(task)


async def bot(runner_args: RunnerArguments):
    """Main bot entry point for the bot starter."""
    transport_type, call_data = await parse_telephony_websocket(runner_args.websocket)
    logger.info(f"Auto-detected transport: {transport_type}")

    serializer = TwilioFrameSerializer(
        stream_sid=call_data["stream_id"],
        call_sid=call_data["call_id"],
        account_sid=os.getenv("TWILIO_ACCOUNT_SID", ""),
        auth_token=os.getenv("TWILIO_AUTH_TOKEN", ""),
    )

    transport = FastAPIWebsocketTransport(
        websocket=runner_args.websocket,
        params=FastAPIWebsocketParams(
            audio_in_enabled=True,
            audio_out_enabled=True,
            add_wav_header=False,
            vad_analyzer=SileroVADAnalyzer(),
            serializer=serializer,
        ),
    )

    await run_bot(transport, call_data["call_id"])


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
