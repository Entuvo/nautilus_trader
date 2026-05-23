import pytest
from datetime import date
from decimal import Decimal

from nautilus_trader.adapters.thetadata.symbology import DecodeError, decode_occ, encode_occ


class TestEncodeOcc:
    def test_standard_symbol(self):
        sym = encode_occ("SPXW", date(2025, 3, 15), "C", Decimal("4800.00"))
        assert sym == "SPXW250315C04800000"

    def test_aapl_standard(self):
        sym = encode_occ("AAPL", date(2024, 10, 18), "P", Decimal("150.00"))
        assert sym == "AAPL241018P00150000"

    def test_sub_dollar_strike(self):
        sym = encode_occ("XYZ", date(2025, 1, 17), "C", Decimal("0.50"))
        assert sym == "XYZ250117C00000500"

    def test_five_digit_strike(self):
        sym = encode_occ("ABC", date(2025, 6, 20), "P", Decimal("99999.99"))
        assert sym == "ABC250620P99999990"

    def test_invalid_root_empty(self):
        with pytest.raises(DecodeError, match="root"):
            encode_occ("", date(2025, 1, 1), "C", Decimal("100"))

    def test_invalid_root_long(self):
        with pytest.raises(DecodeError, match="root"):
            encode_occ("TOOLONG", date(2025, 1, 1), "C", Decimal("100"))

    def test_invalid_root_non_alnum(self):
        with pytest.raises(DecodeError, match="root"):
            encode_occ("A@B", date(2025, 1, 1), "C", Decimal("100"))

    def test_invalid_right(self):
        with pytest.raises(DecodeError, match="right"):
            encode_occ("AAPL", date(2025, 1, 1), "X", Decimal("100"))


class TestDecodeOcc:
    def test_roundtrip_standard(self):
        sym = "SPXW250315C04800000"
        root, expiry, right, strike = decode_occ(sym)
        assert root == "SPXW"
        assert expiry == date(2025, 3, 15)
        assert right == "C"
        assert strike == Decimal("4800.00")

    def test_roundtrip_aapl(self):
        sym = "AAPL241018P00150000"
        root, expiry, right, strike = decode_occ(sym)
        assert root == "AAPL"
        assert expiry == date(2024, 10, 18)
        assert right == "P"
        assert strike == Decimal("150.00")

    def test_roundtrip_sub_dollar(self):
        sym = "XYZ250117C00000500"
        root, expiry, right, strike = decode_occ(sym)
        assert strike == Decimal("0.50")

    def test_malformed_too_short(self):
        with pytest.raises(DecodeError, match="Malformed"):
            decode_occ("SPX250315C0480000")

    def test_malformed_wrong_right(self):
        with pytest.raises(DecodeError, match="Malformed"):
            decode_occ("SPXW250315X04800000")

    def test_malformed_empty(self):
        with pytest.raises(DecodeError, match="Malformed"):
            decode_occ("")


class TestEncodeDecodeRoundtrip:
    def test_roundtrip_various(self):
        cases = [
            ("SPXW", date(2025, 3, 15), "C", Decimal("4800.00")),
            ("AAPL", date(2024, 10, 18), "P", Decimal("150.00")),
            ("XSP", date(2025, 12, 19), "C", Decimal("25.50")),
            ("SPY", date(2025, 1, 3), "P", Decimal("0.01")),
        ]
        for root, expiry, right, strike in cases:
            sym = encode_occ(root, expiry, right, strike)
            r_root, r_expiry, r_right, r_strike = decode_occ(sym)
            assert r_root == root.upper()
            assert r_expiry == expiry
            assert r_right == right
            assert r_strike == strike
