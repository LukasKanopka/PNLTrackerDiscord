from pnl_analyzer.extraction.signals import extract_size_usd


def test_extract_size_usd_ignores_strike_prices() -> None:
    # Strike/threshold in the question should not be treated as wager size.
    txt = "Prediction: Will Ethereum drop below $1900 between Feb 16 to 22?"
    assert extract_size_usd(txt) is None


def test_extract_size_usd_accepts_explicit_sizing_cues() -> None:
    assert extract_size_usd("Total bet: $90") == 90.0
    assert extract_size_usd("Position size $250 on this") == 250.0
