"""
Tests for nautilus_trader.adapters.thetadata.symbology — OCC encode/decode.
"""

from datetime import date
from decimal import Decimal

import pytest

from nautilus_trader.adapters.thetadata.symbology import DecodeError, decode_occ, encode_occ


class TestEncodeOcc:
    def test_spxw_weekly(self):
        assert encode_occ("SPXW", date(2025, 3, 15), "C", Decimal("480.00")) == "SPXW250315C00480000"

    def test_aapl_standard_call(self):
        assert encode_occ("AAPL", date(2024, 6, 21), "C", Decimal("175.50")) == "AAPL240621C00175500"

    def test_aapl_zero_dte_put(self):
        assert encode_occ("AAPL", date(2024, 6, 21), "P", Decimal("175.00")) == "AAPL240621P00175000"

    def test_sub_dollar_strike(self):
        assert encode_occ("XYZ", date(2024, 1, 19), "C", Decimal("0.50")) == "XYZ240119C00000500"

    def test_root_lowercased_uppercases(self):
        assert encode_occ("spxw", date(2025, 3, 15), "C", Decimal("480.00")) == "SPXW250315C00480000"

    def test_root_too_long_raises(self):
        with pytest.raises(DecodeError, match="Invalid OCC root"):
            encode_occ("TOOLONG", date(2025, 3, 15), "C", Decimal("100"))

    def test_root_empty_raises(self):
        with pytest.raises(DecodeError, match="Invalid OCC root"):
            encode_occ("", date(2025, 3, 15), "C", Decimal("100"))

    def test_root_non_alnum_raises(self):
        with pytest.raises(DecodeError, match="Invalid OCC root"):
            encode_occ("SPX-W", date(2025, 3, 15), "C", Decimal("100"))

    def test_invalid_right_raises(self):
        with pytest.raises(DecodeError, match="Invalid OCC right"):
            encode_occ("SPXW", date(2025, 3, 15), "X", Decimal("100"))

    def test_strike_negative_raises(self):
        with pytest.raises(DecodeError, match="Strike out of OCC range"):
            encode_occ("SPXW", date(2025, 3, 15), "C", Decimal("-1.00"))

    def test_strike_too_large_raises(self):
        with pytest.raises(DecodeError, match="Strike out of OCC range"):
            encode_occ("SPXW", date(2025, 3, 15), "C", Decimal("100000.00"))


class TestDecodeOcc:
    def test_roundtrip_spxw(self):
        root, expiry, right, strike = decode_occ("SPXW250315C00480000")
        assert root == "SPXW"
        assert expiry == date(2025, 3, 15)
        assert right == "C"
        assert strike == Decimal("480.000")

    def test_roundtrip_aapl(self):
        root, expiry, right, strike = decode_occ("AAPL240621C00175500")
        assert root == "AAPL"
        assert expiry == date(2024, 6, 21)
        assert right == "C"
        assert strike == Decimal("175.500")

    def test_roundtrip_via_encode(self):
        encoded = encode_occ("SPXW", date(2025, 3, 15), "C", Decimal("480.00"))
        root, expiry, right, strike = decode_occ(encoded)
        assert (root, expiry, right) == ("SPXW", date(2025, 3, 15), "C")
        assert strike == Decimal("480.000")

    def test_malformed_raises(self):
        with pytest.raises(DecodeError, match="Malformed OCC"):
            decode_occ("NOT_OCC")

    def test_empty_raises(self):
        with pytest.raises(DecodeError, match="Malformed OCC"):
            decode_occ("")

    def test_short_strike_raises(self):
        with pytest.raises(DecodeError, match="Malformed OCC"):
            decode_occ("SPXW250315C0048000")
