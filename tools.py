"""LLM-callable tools for claim ID generation and answer storage."""

import random
import string
from typing import Optional

from loguru import logger

# Global state to track current claim ID per call
_claim_ids: dict[str, str] = {}


def set_claim_id(call_sid: str, claim_id: str) -> None:
    """Store the claim ID for a specific call."""
    _claim_ids[call_sid] = claim_id
    logger.info(f"Set claim ID for call {call_sid}: {claim_id}")


def get_claim_id(call_sid: str) -> Optional[str]:
    """Retrieve the claim ID for a specific call."""
    return _claim_ids.get(call_sid)


async def generate_claim_id(call_sid: str) -> str:
    """Generate a unique claim ID (format: 2-3 letters, 10 digits with 000)."""
    # Generate 2-3 random uppercase letters
    prefix = "".join(random.choices(string.ascii_uppercase, k=random.choice([2, 3])))

    # Generate 10 digits with three consecutive zeros at random position
    zero_position = random.randint(0, 7)
    digits = list(random.choices(string.digits, k=10))
    digits[zero_position : zero_position + 3] = ["0", "0", "0"]

    claim_id = f"{prefix}{''.join(digits)}"
    set_claim_id(call_sid, claim_id)
    logger.info(f"Generated claim ID: {claim_id}")
    return claim_id
