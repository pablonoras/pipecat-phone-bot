import asyncio
import os

from dotenv import load_dotenv
from loguru import logger
from pipecat.adapters.schemas.function_schema import FunctionSchema
from pipecat.adapters.schemas.tools_schema import ToolsSchema
from pipecat.audio.vad.silero import SileroVADAnalyzer
from pipecat.frames.frames import LLMRunFrame
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
from pipecat.transports.daily.transport import DailyParams, DailyTransport
from pipecat.transports.websocket.fastapi import (
    FastAPIWebsocketParams,
    FastAPIWebsocketTransport,
)

from tools import generate_claim_id

load_dotenv(override=True)


async def run_bot(transport: BaseTransport, call_sid: str):
    logger.info(f"Starting bot for call {call_sid}")

    stt = DeepgramSTTService(
        api_key=os.getenv("DEEPGRAM_API_KEY"), interim_results=True
    )
    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        voice_id="f9836c6e-a0bd-460e-9d3c-f7299fa60f94",  # British Reading Lady
    )
    llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"), streaming=True)

    # Generate claim_id at call start and make it available to the prompt
    claim_id = await generate_claim_id(call_sid)

    # No tools; we'll buffer locally and upload once at the end
    tools = ToolsSchema(standard_tools=[])

    # Upload guard to avoid double writes
    uploaded = {"value": False}

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


async def bot(runner_args):
    """Main bot entry point for the bot starter."""

    # Detect mode: WebSocket (Twilio) or Daily (Sandbox)
    if hasattr(runner_args, "websocket") and runner_args.websocket:
        # ===== WEBSOCKET MODE (Twilio via ngrok or Pipecat Cloud) =====
        logger.info("Running in WebSocket mode (Twilio)")
        transport_type, call_data = await parse_telephony_websocket(
            runner_args.websocket
        )
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
        call_sid = call_data["call_id"]

    else:
        # ===== DAILY MODE (Sandbox/Testing) =====
        logger.info("Running in Daily mode (Sandbox)")

        # Manually create Daily transport
        transport = DailyTransport(
            runner_args.room_url,
            runner_args.token,
            "phone-bot-example",
            DailyParams(
                audio_in_enabled=True,
                audio_out_enabled=True,
                vad_analyzer=SileroVADAnalyzer(),
            ),
        )
        call_sid = getattr(runner_args, "session_id", "phone-bot-example")

    await run_bot(transport, call_sid)


if __name__ == "__main__":
    from pipecat.runner.run import main

    main()
