# Load profile and bottleneck analysis

The README profile was run against the public gateway with 200 requests and 8
workers. It returned 82 HTTP 200 results and 118 status `0` results, with P50 8.13
ms, P95 1776.44 ms and P99 4809.09 ms. The supplied standard-library probe maps
all `HTTPError` responses, including the intentional Envoy 429 response, to `0`.
The separate gateway integration test proved the 429 response, request ID, refill
and rate-limit counter, so this result records edge protection rather than raw app
capacity.

The same profile was run directly against `http://localhost:8000` to isolate the
application: 200/200 responses were HTTP 200; P50 was 573.11 ms, P95 775.97 ms and
P99 939.46 ms. The main remaining cost is `/ready` probing Kafka, MLflow, Qdrant,
Feast and the optional inference endpoint on each call. Short-lived probe caching,
strict dependency budgets and separate shallow/deep readiness endpoints would
reduce tail latency.

Raw results: `evidence/load-profile.json` and `evidence/load-profile-api.json`.
