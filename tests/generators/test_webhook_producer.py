from unittest.mock import MagicMock, patch

from src.generators.webhook_producer import (
    delivery_report,
    generate_webhook_event,
    main,
)


def test_generate_webhook_event():
    event = generate_webhook_event()
    assert "transaction_id" in event
    assert event["transaction_id"].startswith("tx_")
    assert "amount_paise" in event
    assert isinstance(event["amount_paise"], int)
    assert 1000 <= event["amount_paise"] <= 1000000
    assert event["gateway_status"] == "SUCCESS"
    assert "timestamp_utc" in event
    assert "merchant_id" in event


def test_delivery_report_success(capsys):
    msg = MagicMock()
    msg.topic.return_value = "gateway_webhooks"
    msg.partition.return_value = 0
    msg.offset.return_value = 42

    delivery_report(None, msg)

    captured = capsys.readouterr()
    assert "Produced record to gateway_webhooks [0] @ offset 42" in captured.out


def test_delivery_report_failure(capsys):
    err = MagicMock()
    err.__str__ = lambda self: "broker down"

    delivery_report(err, None)

    captured = capsys.readouterr()
    assert "Delivery failed:" in captured.out


@patch("src.generators.webhook_producer.time.sleep", side_effect=KeyboardInterrupt)
@patch("src.generators.webhook_producer.SerializingProducer")
@patch("src.generators.webhook_producer.SchemaRegistryClient")
@patch("src.generators.webhook_producer.argparse.ArgumentParser")
def test_main_continuous_loop(mock_parser_cls, mock_registry_cls, mock_producer_cls, mock_sleep):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value = MagicMock(stress=0)
    mock_parser_cls.return_value = mock_parser

    main()

    mock_producer_cls.return_value.produce.assert_called()
    mock_producer_cls.return_value.flush.assert_called_once()


@patch("src.generators.webhook_producer.time.time", side_effect=[0.0, 1.0])
@patch("src.generators.webhook_producer.SerializingProducer")
@patch("src.generators.webhook_producer.SchemaRegistryClient")
@patch("src.generators.webhook_producer.argparse.ArgumentParser")
def test_main_stress_mode(mock_parser_cls, mock_registry_cls, mock_producer_cls, mock_time):
    mock_parser = MagicMock()
    mock_parser.parse_args.return_value = MagicMock(stress=10)
    mock_parser_cls.return_value = mock_parser

    main()

    assert mock_producer_cls.return_value.produce.call_count == 10
    mock_producer_cls.return_value.flush.assert_called_once()
