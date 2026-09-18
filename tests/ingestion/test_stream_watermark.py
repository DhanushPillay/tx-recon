import pytest

from src.common.settings import Settings
from src.ingestion.ingest_webhooks import parse_watermark_delay


@pytest.mark.parametrize(
    ("raw", "want"),
    [
        ("1 day", (1, "day")),
        ("12 hours", (12, "hours")),
        ("30 minutes", (30, "minutes")),
        ("90 seconds", (90, "seconds")),
        ("  2 DAYS  ", (2, "days")),
    ],
)
def test_parse_watermark_delay_valid(raw, want):
    assert parse_watermark_delay(raw) == want


@pytest.mark.parametrize(
    "raw", ["", "soon", "1 fortnight", "1 day; DROP TABLE x", "-3 hours", "1.5 days"]
)
def test_parse_watermark_delay_rejects(raw):
    with pytest.raises(ValueError):
        parse_watermark_delay(raw)


def test_stream_watermark_delay_default_parses():
    assert parse_watermark_delay(Settings().stream_watermark_delay) == (1, "day")
