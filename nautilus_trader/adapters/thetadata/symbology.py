import re
from datetime import date
from decimal import Decimal


class DecodeError(Exception):
    pass


OCC_PATTERN = re.compile(r'^([A-Za-z]{1,5})(\d{6})([CP])(\d{8})$')


def encode_occ(root: str, expiry: date, right, strike: Decimal) -> str:
    """Encode OCC symbol. right is OptionKind.CALL or OptionKind.PUT. strike in dollars -> thousandths (x1000, 8-digit zero-padded)."""
    if not re.match(r'^[A-Za-z]{1,5}$', root):
        raise DecodeError(f"Invalid OCC root: {root!r}")
    if right not in ("C", "P"):
        raise DecodeError(f"Invalid OCC right: {right!r}")
    strike_thousandths = int(strike * 1000)
    if not (0 <= strike_thousandths <= 99999999):
        raise DecodeError(f"Strike out of OCC range: {strike_thousandths}")
    return f"{root.upper()}{expiry.strftime('%y%m%d')}{right}{strike_thousandths:08d}"


def decode_occ(symbol: str) -> tuple[str, date, str, Decimal]:
    """Decode OCC symbol. Returns (root, expiry, right, strike_dollars)."""
    m = OCC_PATTERN.match(symbol.upper())
    if not m:
        raise DecodeError(f"Malformed OCC symbol: {symbol!r}")
    root, date_str, right, strike_str = m.groups()
    expiry = date(2000 + int(date_str[:2]), int(date_str[2:4]), int(date_str[4:6]))
    strike_dollars = Decimal(int(strike_str)) / Decimal(1000)
    return root, expiry, right, strike_dollars
