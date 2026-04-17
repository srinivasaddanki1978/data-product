"""Formatting utilities for the Cost Optimization Dashboard."""


def format_currency(value, decimals=2):
    """Format a number as USD currency."""
    if value is None:
        return "$0.00"
    if abs(value) >= 1_000_000:
        return f"${value / 1_000_000:,.{decimals}f}M"
    if abs(value) >= 1_000:
        return f"${value / 1_000:,.{decimals}f}K"
    return f"${value:,.{decimals}f}"


def format_pct(value, decimals=1):
    """Format a number as a percentage."""
    if value is None:
        return "0.0%"
    return f"{value:,.{decimals}f}%"


def format_bytes(value):
    """Format bytes into human-readable units."""
    if value is None or value == 0:
        return "0 B"
    units = ["B", "KB", "MB", "GB", "TB", "PB"]
    idx = 0
    v = float(value)
    while v >= 1024 and idx < len(units) - 1:
        v /= 1024
        idx += 1
    return f"{v:,.2f} {units[idx]}"


def format_number(value, decimals=0):
    """Format a number with comma separators."""
    if value is None:
        return "0"
    return f"{value:,.{decimals}f}"


def change_badge(value):
    """Return a colored delta indicator for Streamlit metrics."""
    if value is None or value == 0:
        return "0.0%"
    return f"{value:+.1f}%"
