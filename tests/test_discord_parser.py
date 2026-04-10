from pnl_analyzer.parsing.discord_txt import parse_discord_txt


def test_bracket_header_parses_and_converts_to_utc() -> None:
    content = "[3/18/2026 10:15 PM] alice: buying YES 48c\n[3/18/2026 10:16 PM] bob: ok\n"
    msgs = parse_discord_txt(content, export_timezone="America/New_York")
    assert len(msgs) == 2
    assert msgs[0]["author"] == "alice"
    # 2026-03-18 22:15 America/New_York is 2026-03-19 02:15Z (DST)
    assert msgs[0]["timestamp_utc"] == "2026-03-19T02:15:00Z"


def test_dash_header_parses_multiline() -> None:
    content = "3/18/2026 10:15 PM - alice: line1\nline2\n3/18/2026 10:16 PM - bob: hi\n"
    msgs = parse_discord_txt(content, export_timezone="America/New_York")
    assert len(msgs) == 2
    assert msgs[0]["text"] == "line1\nline2"


def test_em_dash_header_parses_following_lines() -> None:
    content = "alice — 3/18/2026 10:15 PM\nhello\nworld\nbob — 3/18/2026 10:16 PM\nok\n"
    msgs = parse_discord_txt(content, export_timezone="America/New_York")
    assert len(msgs) == 2
    assert msgs[0]["text"] == "hello\nworld"

