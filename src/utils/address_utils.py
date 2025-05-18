import logging
from typing import Tuple

logger = logging.getLogger("address_utils")


def parse_address(address: str) -> Tuple[str, str, str]:
    """
    Parse an address into its components, handling edge cases like leading zeros and ranges.

    Args:
        address: Raw address string (e.g., "0211-213 E BALTIMORE ST")

    Returns:
        Tuple of (cleaned_address, address_number, street_name)
    """
    # Trim whitespace
    address = address.strip()

    # Split address into parts
    parts = address.split(" ", 1)
    if len(parts) < 2:
        return address, "", address  # No clear separation, return as is

    # Get number part and street part
    number_part = parts[0]
    street_part = parts[1]

    # Handle number part with dashes (address ranges)
    if "-" in number_part:
        # Take only the first number in the range
        range_parts = number_part.split("-", 1)
        number_part = range_parts[0]

    # Remove leading zeros from number part
    if number_part.startswith("0"):
        number_part = number_part.lstrip("0")
        if not number_part:
            number_part = "0"  # Keep at least one zero if number was all zeros

    # Build cleaned address
    cleaned_address = f"{number_part} {street_part}"

    return cleaned_address, number_part, street_part


def get_simplified_address(address: str) -> str:
    """
    Get a simplified version of the address (just number and first part of street name).
    Useful for more flexible searches.

    Args:
        address: Raw address string

    Returns:
        Simplified address (e.g., "211 BALTIMORE")
    """
    _, number, street = parse_address(address)

    if not number or not street:
        return address

    # Take just the first part of the street name
    street_parts = street.split()
    if not street_parts:
        return f"{number}"

    return f"{number} {street_parts[0]}"


def format_address_for_display(address: str) -> str:
    """
    Format an address for display, with proper capitalization.

    Args:
        address: Raw address string

    Returns:
        Formatted address with proper capitalization
    """
    cleaned, number, street = parse_address(address)

    # Capitalize street name words
    street_parts = street.split()
    capitalized_parts = []

    # Common words that should be lowercase unless they're the first word
    lowercase_words = {
        "a",
        "an",
        "the",
        "and",
        "but",
        "or",
        "nor",
        "at",
        "by",
        "for",
        "from",
        "in",
        "into",
        "of",
        "off",
        "on",
        "onto",
        "out",
        "over",
        "to",
        "up",
        "with",
    }

    # Directional abbreviations that should be uppercase
    directionals = {"n", "s", "e", "w", "ne", "nw", "se", "sw"}

    for i, part in enumerate(street_parts):
        part_lower = part.lower()

        # Handle directionals
        if part_lower in directionals:
            capitalized_parts.append(part_lower.upper())
        # Handle first word or non-lowercase words
        elif i == 0 or part_lower not in lowercase_words:
            capitalized_parts.append(part.title())
        # Handle lowercase words
        else:
            capitalized_parts.append(part_lower)

    return f"{number} {' '.join(capitalized_parts)}"
