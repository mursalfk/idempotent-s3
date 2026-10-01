"""
Local scaffolding so the whole demo runs offline, no AWS account.

s3_event() builds a record shaped exactly like a real S3-to-Lambda notification,
including the fields the idempotency keys rely on: key, eTag, versionId, sequencer.

FakeTable is a tiny stand-in for a DynamoDB table that supports the one feature
the fix depends on: a conditional write that fails if the item already exists,
which is how a real DynamoDB put_item(ConditionExpression="attribute_not_exists")
makes a retried invocation a safe no-op.
"""


def s3_event(key="invoices/inv-4471.json", etag="d41d8cd98f00b204", version_id="v1",
             sequencer="00A1B2C3D4E5F6", bucket="demo-bucket"):
    return {
        "Records": [{
            "eventName": "ObjectCreated:Put",
            "s3": {
                "bucket": {"name": bucket},
                "object": {
                    "key": key,
                    "eTag": etag,
                    "versionId": version_id,
                    "sequencer": sequencer,
                },
            },
        }]
    }


class ConditionalCheckFailed(Exception):
    pass


class FakeTable:
    def __init__(self):
        self.items = {}

    def put_if_absent(self, key, item):
        # mirrors put_item(ConditionExpression="attribute_not_exists(pk)")
        if key in self.items:
            raise ConditionalCheckFailed(key)
        self.items[key] = item

    def increment_counter(self, pk, field):
        row = self.items.setdefault(pk, {})
        row[field] = row.get(field, 0) + 1
        return row[field]

    def get(self, pk):
        return self.items.get(pk)
