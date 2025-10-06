"""Pipecat Twilio Phone Example.

The example runs a simple voice AI bot that you can connect to using a
phone via Twilio.

Required AI services:
- Deepgram (Speech-to-Text)
- OpenAI (LLM)
- Cartesia (Text-to-Speech)

The example connects between client and server using a Twilio websocket
connection.

Run the bot using::

    uv run bot.py -t twilio -x your_ngrok.ngrok.io
"""

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
from pipecat.frames.frames import EndFrame, LLMRunFrame

from tools import generate_claim_id, store_answer

load_dotenv(override=True)


SCRIPT_PROMPT = (
    "You are a friendly caller interacting with a human agent about an insurance claim. "
    "Follow this flow exactly, stay concise and natural:\n"
    "1) If greeted or asked how to help, say: 'I need information about a claim.'\n"
    "2) When asked for the claim number, generate and speak a claim number that:\n"
    "   - contains exactly 10 digits\n"
    "   - includes three consecutive zeros (000)\n"
    "   - may be prefixed by 2–3 uppercase letters and an optional dash\n"
    "   Example formats: 'QJ-7400005183', 'AB000123456'. Speak only the claim number.\n"
    "3) When they confirm they've found the claim, ask these one at a time, waiting for an answer after each:\n"
    "   a) 'When was the claim submitted?'\n"
    "   b) 'What is the status?'\n"
    "   c) 'What is the claim number?'\n"
    "4) After receiving the answer to the last question (claim number), say ONLY: 'Got it, thank you, and have a nice day.'\n"
    "Always wait for the human to finish before speaking. Do not ask multiple questions at once. "
    "If they go off-script, politely steer back to the required questions."
)


async def run_bot(transport: BaseTransport, call_sid: str):
    logger.info(f"Starting bot for call {call_sid}")

    stt = DeepgramSTTService(api_key=os.getenv("DEEPGRAM_API_KEY"))
    tts = CartesiaTTSService(
        api_key=os.getenv("CARTESIA_API_KEY"),
        voice_id="71a7ad14-091c-4e8e-a314-022ece01c121",  # British Reading Lady
    )
    llm = OpenAILLMService(api_key=os.getenv("OPENAI_API_KEY"))

    # Task reference for ending the call
    task_ref = {"task": None}

    async def end_call_after_delay():
        """End the call after 2 seconds to let TTS finish."""
        await asyncio.sleep(2)
        logger.info("Ending call...")
        if task_ref["task"]:
            await task_ref["task"].queue_frame(EndFrame())

    # Define function schemas
    tools = ToolsSchema(
        standard_tools=[
            FunctionSchema(
                name="generate_claim_id",
                description="Generate a unique claim ID for this conversation",
                properties={},
                required=[],
            ),
            FunctionSchema(
                name="store_answer",
                description="Store an answer to a claim question",
                properties={
                    "question": {"type": "string", "description": "The question asked"},
                    "answer": {"type": "string", "description": "The answer provided"},
                },
                required=["question", "answer"],
            ),
        ]
    )

    # Register function handlers
    async def generate_claim_id_handler(params):
        result = await generate_claim_id(call_sid)
        await params.result_callback(result)

    async def store_answer_handler(params):
        question = params.arguments.get("question", "")
        answer = params.arguments.get("answer", "")

        # Check if this is the final question BEFORE storing
        is_final = "claim number" in question.lower()

        # Fire-and-forget to reduce latency
        asyncio.create_task(store_answer(call_sid, question, answer))

        if is_final:
            logger.info("Final question answered, will end call after goodbye")
            asyncio.create_task(end_call_after_delay())
            await params.result_callback("Final answer recorded. Say goodbye and end call.")
        else:
            await params.result_callback("Answer recorded")

    llm.register_function("generate_claim_id", generate_claim_id_handler)
    llm.register_function("store_answer", store_answer_handler)

    messages = [{"role": "system", "content": SCRIPT_PROMPT}]
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
