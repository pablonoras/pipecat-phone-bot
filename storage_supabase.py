"""Supabase storage module for claim records."""

import os
from typing import Optional

from loguru import logger
from supabase import Client, create_client

# Initialize Supabase client
_supabase_client: Optional[Client] = None


def get_supabase_client() -> Client:
    """Get or initialize the Supabase client."""
    global _supabase_client

    if _supabase_client is None:
        url = os.getenv("SUPABASE_URL")
        key = os.getenv("SUPABASE_KEY")

        if not url or not key:
            raise ValueError("SUPABASE_URL and SUPABASE_KEY must be set in environment")

        _supabase_client = create_client(url, key)
        logger.info("Supabase client initialized")

    return _supabase_client


async def add_conversation_record(
    call_sid: str, bot_messages: list[str], user_messages: list[str]
) -> bool:
    # Deprecated: kept for backward compatibility if called elsewhere; do nothing
    logger.info(
        "add_conversation_record is deprecated; use add_conversation_messages instead"
    )
    return True


async def add_conversation_messages(call_sid: str, messages: list[dict]) -> bool:
    """
    Insert multiple conversation messages (one row per role/content) into 'claims'.

    Each message dict should contain keys: 'role' and 'content'.
    The function will add 'call_sid' before inserting.

    Args:
        call_sid: The Twilio call SID
        messages: List of role/content dicts

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_supabase_client()

        rows = []
        for m in messages:
            role = m.get("role", "")
            content = m.get("content", "")
            if not content:
                continue
            rows.append({"call_sid": call_sid, "role": role, "content": content})

        if not rows:
            logger.info("No conversation messages to store")
            return True

        client.table("claims").insert(rows).execute()
        logger.info(f"Stored {len(rows)} conversation messages for call {call_sid}")
        return True
    except Exception as e:
        logger.error(f"Failed to store conversation messages: {e}")
        return False
