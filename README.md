<!-- README.md -->
# idempotent-s3

An S3-triggered Lambda that charges a customer will, eventually, charge them twice. Not because of a logic bug, but because S3-to-Lambda delivery is at-least-once and Lambda retries failed async invocations. This repo forces that duplicate on demand, then compares three idempotency patterns: one that is broken, one that is subtly broken, and one that is correct.

Article: *The Ledger That Charged Twice: S3 Lambda Idempotency* (link coming once it is published).

## The whole story in one table

Same upload, run through Lambda's async-retry behaviour (`python -m local.sim`):

```
scenario: transient   (want: charged exactly once)
  handler                         charge count   verdict
  naive                                      2   double charged
  safe_a  (key + versionId)                  1   charged once
  safe_b  (key + eTag + seq)                 1   charged once
  robust  (claim/work/commit)                1   charged once

scenario: poison      (want: not silently dropped)
  handler                         charge count   verdict
  naive                                      3   charged 3
  safe_a  (key + versionId)                  1   charged 1
  safe_b  (key + eTag + seq)                 1   charged 1
  robust  (claim/work/commit)                0   charge dropped
```

The naive handler double-charges on a blip and triple-charges on a persistent failure. The claim-first handlers (`safe_a`, `safe_b`) fix the double charge, but in the poison case they claim the key, move money, and then the operation still fails. Only the robust handler is correct in both cases: it charges nothing and leaves an `in_progress` record for the dead-letter queue.

## Why it happens

Two independent duplicate sources, both documented by AWS:

1. **Lambda async retry.** S3 invokes Lambda asynchronously, and async invocations are retried up to two more times by default (`MaximumRetryAttempts`, 0 to 2). A handler that does a side effect and then errors is re-invoked, and the side effect runs again. This is deterministic and is the demo in this repo.
2. **S3 at-least-once delivery.** The S3 docs say event notifications are delivered at least once and recommend you account for duplicate events. This is rarer and non-deterministic, so the repo designs for it rather than staging it.

The `sequencer` field on an S3 event orders events for the same object but does not deduplicate them. Deduplication is your job.

## The three patterns

- **naive**: does the charge, no idempotency. Broken under any retry.
- **claim-first** (`safe_a`, `safe_b`): claim an idempotency key with a conditional write before charging, so a retry is skipped. Two key designs: object key + `versionId` (needs bucket versioning), or a hash of key + `eTag` + `sequencer` (no versioning). Fixes the double charge, but can leave the money state inconsistent if the charge fails after the claim.
- **robust**: claim as `in_progress`, charge, then mark `done`. A retry that finds `in_progress` tries again; one that finds `done` skips. Genuine repeated failures fall through to the DLQ instead of being silently deduplicated.

## Layout

```
local/       offline simulator (FakeTable, fake S3 events, sim.py)
pipeline/    the real thing: handlers.py, lambda_function.py, template.yaml, query_state.py
tests/       7 pytest tests, one per row of the table
events/      sample S3 objects to upload (charge.json, charge2.json, r1.json)
```

## Run it locally (no AWS)

```bash
python -m venv .venv
source .venv/bin/activate        # Windows (Git Bash): source .venv/Scripts/activate
pip install -e ".[dev]"

python -m local.sim              # prints the table above
python -m pytest tests/ -v       # 7 passed
```

## Deploy the real pipeline

Needs the AWS SAM CLI and credentials that can create S3, Lambda, DynamoDB, SQS and IAM resources. The default region in `samconfig.toml` is `eu-west-3`; change it if you like.

```bash
export AWS_PROFILE=<your-deploy-profile>     # PowerShell: $env:AWS_PROFILE="<your-deploy-profile>"
sam build -t pipeline/template.yaml
sam deploy
```

`sam deploy` reads `samconfig.toml`. The stack outputs the bucket name and the table name. Then upload a file and read the counter:

```bash
export AWS_REGION=eu-west-3
export STATE_TABLE=<StateTable output>
aws s3 cp events/charge.json s3://<BucketName output>/charge.json
python pipeline/query_state.py
```

To flip between patterns, redeploy with a different `HandlerMode` (`naive`, `safe` or `robust`). `FailOnce=true` forces the first attempt to fail so Lambda retries, which is how the duplicate shows up on real Lambda. Use a fresh file name for each run, because the idempotency record is per object.

Tear everything down with `sam delete`. It costs next to nothing, but delete it anyway.

## License

MIT.