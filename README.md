## AI Voice Agent (Pipecat + Twilio)

Phone-accessible voice agent using Pipecat. The bot runs with Deepgram (STT), OpenAI (LLM), Cartesia (TTS) and Twilio Media Streams.

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

5. Run

```bash
uv run bot.py --transport twilio --proxy <your-ngrok>.ngrok.io
```

## Setup (online, production)

This project is configured for deployment on **Pipecat Cloud**:

1. **Deploy to Pipecat Cloud:**

   - Configure `pcc-deploy.toml` with your service settings
   - Deploy using `pcc deploy` command
   - Pipecat Cloud will provide a secure WebSocket endpoint URL

2. **Configure Twilio TwiML Bin:**

   - Go to Twilio Console → TwiML Bins → Create new
   - Add TwiML code that connects to your Pipecat Cloud WebSocket endpoint:
     ```xml
     <?xml version="1.0" encoding="UTF-8"?>
     <Response>
       <Connect>
         <Stream url="wss://your-pipecat-cloud-endpoint.pipecat.ai" />
          <Parameter name="_pipecatCloudServiceHost" value="{bot-name}.{org-name}"/>
       </Connect>
     </Response>
     ```
   - Save the TwiML Bin and copy its URL

3. **Point Twilio number to TwiML Bin:**

   - Go to your Twilio phone number settings
   - Voice → "A call comes in" → TwiML Bin → Select your bin
   - Save configuration

4. **Environment variables:**
   - Configure secrets in Pipecat Cloud dashboard (no `.env` in production)
   - Required: API keys for Deepgram, OpenAI, Cartesia, and Supabase credentials

Call your Twilio number to talk to the bot.

## Supabase Integration

The bot stores conversation data (call_sid, role, content, created_at) in Supabase.

**Database Schema:**

Create a `claims` table with:

- `id` (uuid, primary key, default: `gen_random_uuid()`)
- `call_sid` (text)
- `role` (text) - either 'user' or 'assistant'
- `content` (text) - the message content
- `created_at` (timestamptz, default: `now()`)

View stored conversations at supabase public endpoint using the public key.

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

- New added behavior: The Bot will check if the claim number correspond to the user answer of the last question, if so, it will say "Got it, thank you, and have a nice day." if not it will tell the number again and repeat the 3 questions.

- No hangup logic added for simplicity (out of the scope).
