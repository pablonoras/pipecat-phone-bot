## Prosper AI Voice Agent (Pipecat + Twilio)

Build a phone-accessible voice agent using Pipecat. The bot runs with Deepgram (STT), OpenAI (LLM), Cartesia (TTS) and Twilio Media Streams.

## Prerequisites

- Python 3.10+
- [uv](https://docs.astral.sh/uv/getting-started/installation/) package manager installed
- [ngrok](https://ngrok.com/docs/getting-started/) (for tunneling)
- Twilio account and phone number
- API keys: Deepgram, OpenAI, Cartesia
- Supabase account (for conversation storage)

## Setup (local, via ngrok)

1. Create `.env` and add keys:

   ```bash
   cp env.example .env
   ```

   Required: `DEEPGRAM_API_KEY`, `OPENAI_API_KEY`, `CARTESIA_API_KEY`, `SUPABASE_URL`, `SUPABASE_KEY`

   Optional: `TWILIO_ACCOUNT_SID`, `TWILIO_AUTH_TOKEN`

2. Install deps:

   ```bash
   uv sync
   ```

3. Start ngrok (new terminal):

   ```bash
   ngrok http 7860
   ```

4. Configure Twilio number → Voice → "A call comes in" → Webhook → `https://<your-ngrok>.ngrok.io` (POST).

## Setup (online, production)

- Run the server behind a public HTTPS domain (no ngrok). Options:
  - Deploy to your infra (e.g., Docker on a VM, container service, or Kubernetes)
  - Or deploy to Pipecat Cloud (TBD for this project)
- Point Twilio → Voice → "A call comes in" → Webhook to `https://your-domain` (POST)
- Manage secrets via your platform's secret manager (no `.env` in production)
- Ensure TLS certs are valid and choose a region close to Twilio for lower latency

## Run

```bash
uv run bot.py --transport twilio --proxy <your-ngrok>.ngrok.io
```

Call your Twilio number to talk to the bot.

## Supabase Integration

The bot stores conversation data (claim IDs and Q&A pairs) in Supabase.

**Database Schema:**

Create a `claims` table with:
- `id` (uuid, primary key, default: `gen_random_uuid()`)
- `call_sid` (text)
- `claim_id` (text)
- `question` (text)
- `answer` (text)
- `created_at` (timestamptz, default: `now()`)

**View Logs:**

View stored conversations at: `https://<your-project>.supabase.co/project/<project-id>/editor/<table-id>`

Or query via SQL:
```sql
SELECT * FROM claims ORDER BY created_at DESC LIMIT 100;
```

## Scripted flow (challenge)

- User: "Hi how can I help you?"
- Bot: "I need information about a claim."
- User: "Ok, what is the claim number?"
- Bot: Speaks a claim number with exactly 10 digits and "000" consecutively (may be prefixed by 2–3 uppercase letters and optional dash, e.g., "QJ7400005183", "AB000123456").
- User: "Ok, I've found the claim what do you need?"
- Bot: Asks, one by one, waiting for answers:
  1. When was the claim submitted?
  2. What is the status?
  3. What is the claim number?
- Bot: Says "Got it, thank you, and have a nice day." and automatically ends the call after 2 seconds.

## Logging

- Prints concise turns to stdout: `user: ...`, `assistant: ...`.

## Deliverables guidance

- Phone number reachable to test the agent (Twilio).
- Public site for conversation results (e.g., expose recent transcript JSON or simple HTML).
- GitHub repo (this project).
- Short latency evaluation: total; optionally per STT/LLM/TTS. Note ideas: stream early, smaller/faster models, reduce prompt/context, keep RTT low.

## Troubleshooting

- Verify ngrok URL in Twilio config.
- Ensure `.env` keys are set.
- Start ngrok before calling.
