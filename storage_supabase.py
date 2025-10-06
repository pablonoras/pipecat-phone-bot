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


async def add_claim_record(
    call_sid: str, claim_id: str, question: str, answer: str
) -> bool:
    """
    Insert a new claim record into the Supabase 'claims' table.

    Args:
        call_sid: The Twilio call SID
        claim_id: The generated claim ID
        question: The question asked
        answer: The answer provided

    Returns:
        True if successful, False otherwise
    """
    try:
        client = get_supabase_client()

        data = {
            "call_sid": call_sid,
            "claim_id": claim_id,
            "question": question,
            "answer": answer,
        }

        result = client.table("claims").insert(data).execute()
        logger.info(f"Stored claim record: {claim_id} | {question[:50]}...")
        return True

    except Exception as e:
        logger.error(f"Failed to store claim record: {e}")
        return False
