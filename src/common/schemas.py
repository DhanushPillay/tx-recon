WEBHOOK_AVRO_SCHEMA = """
{
  "type": "record",
  "name": "WebhookEvent",
  "fields": [
    {"name": "transaction_id", "type": "string"},
    {"name": "amount_paise", "type": "int"},
    {"name": "gateway_status", "type": "string"},
    {"name": "timestamp_utc", "type": "string"},
    {"name": "merchant_id", "type": "string"}
  ]
}
"""
