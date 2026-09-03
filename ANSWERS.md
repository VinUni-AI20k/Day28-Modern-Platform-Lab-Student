# ANSWERS.md — Day 28 Track 2

- **Sinh viên:** Trần Đình Đăng — `2A202601998`
- **Hình thức:** cá nhân
- **Nhánh làm việc:** `ca-nhan-dang` (PR vào `main`)
- **Máy chạy:** Windows 11, 12 vCPU, 15.7 GiB RAM, NVIDIA RTX 4050 Laptop (6 GiB VRAM)
- **Đường chạy đã dùng:** Bước 1–7 (hệ thống cơ bản). Bước 8–9 không chạy.

> Trạng thái trung thực: hệ thống cơ bản (12 service) **đã chạy thật** và số liệu
> dưới đây là output thật. Bước 8 (`--profile full`: Spark Connect + Airflow) và
> Bước 9 (vLLM/GPU) **không chạy**, nên IP02, IP03-live, IP07 và 6 file evidence
> do integration test sinh ra được ghi `UNVERIFIED`/`not_ready`. Không có số liệu
> nào được bịa hoặc mô phỏng, theo `docs/rubric.md:12-13`.

---

## 1. Bốn ranh giới đã hoàn thiện

| Hàm | IP | Call site thật |
|---|---|---|
| `event_headers` | IP01 + IP10 | `src/lab28_platform/event_bus.py:178` |
| `dedupe_latest` | IP03 | `src/lab28_platform/delta_store.py:206` |
| `feast_online_request` | IP04 | `src/lab28_platform/feature_store.py:104` |
| `readiness_status` | IP07 + IP08 | `src/lab28_platform/readiness.py:241` |

Kiểm tra tĩnh (exit code thật):

| Lệnh | Kết quả |
|---|---|
| `pytest starter-tests -q` (trước khi sửa) | 4 failed — đúng baseline `NotImplementedError` |
| `pytest starter-tests tests -q` | **87 passed** |
| `pytest integration-tests -m offline -q` | 1 passed, 71 deselected |
| `ruff check .` | All checks passed |
| `scripts/verify_matrix.py` | 245 checks passed |
| `scripts/check_portability.py` | OK |
| `scripts/validate_manifests.py` | passed |
| `docker compose config` (base / full / gpu overlay) | exit 0 / 0 / 0 |

## 2. Hệ thống cơ bản đã chạy thật (Bước 7)

`docker compose --env-file ports.template up -d --build --wait` → **12/12 service
healthy**: api, feast, gateway, grafana, jaeger, kafka, kafka-exporter, mlflow,
otel-collector, prometheus, pushgateway, qdrant.

| Lệnh | Output thật |
|---|---|
| `lab28 topics` | 4 topic `created`: `data.raw`, `data.processed`, `model.events`, `data.raw.dlq` |
| `lab28 index --source file` | `points_upserted: 13`, `points_total: 13` |
| `lab28 release` | `lab28-rag-release v2` → alias `champion` |
| `lab28 seed --via-gateway --limit 3` | 0 rejected |
| `lab28 seed` (toàn bộ corpus, 13 doc + 12 feedback) | 0 rejected |
| `lab28 inspect` | kafka 1 broker/4 topic, feast ok, qdrant 13 points, mlflow champion, spark-delta unreachable, vllm unreachable |
| `lab28 ready` | `degraded` (exit 0) |
| `lab28 integration` | **score 67**, exit 1 |

### `/ready` — bằng chứng sống cho `readiness_status`

```
GET http://localhost:8000/ready  → HTTP 200
{"status":"degraded","components":[
  {"name":"kafka","ready":true,...},{"name":"mlflow","ready":true,...},
  {"name":"qdrant","ready":true,...},{"name":"vllm","ready":false,...},
  {"name":"feast","ready":true,...}]}
GET http://localhost:8080/ready  (qua Envoy) → HTTP 200
```

Đây đúng là ngữ nghĩa đã cài: vLLM chết nhưng **không bắt buộc** trong cấu hình
triển khai (`compose.yaml:226` đặt `LAB28_VLLM_REQUIRE_REAL=false`), nên kết quả
là `degraded` chứ không phải `not_ready`, và Envoy **giữ instance trong rotation**
(HTTP 200). Nếu gộp hai trạng thái, ta sẽ mất traffic không cần thiết.

### Điểm số thật từ `evidence/integration-report.json`

`ready=false, verified=6, passing=4, unverified=4, score=67`
(pillars: `operations 1.0`, `reliability 0.6`)

| IP | Status | Detail |
|---|---|---|
| IP01 | `ready` | 1 broker(s); 4 declared topics present |
| IP02 | `unverified` | cần `evidence/ip02-airflow-run.json` (Airflow = profile full) |
| IP03 | `not_ready` | no Delta table — MERGE cần Spark Connect (profile full) |
| IP04 | `ready` | ok |
| IP05 | `ready` | 13 points; ok |
| IP06 | `ready` | `lab28-rag-release v3 is champion` |
| IP07 | `not_ready` | `unreachable: ConnectError` — chủ động không cấu hình vLLM |
| IP08 | `unverified` | cần `evidence/ip08-gateway.json` |
| IP09 | `unverified` | cần `evidence/ip09-*.json` |
| IP10 | `unverified` | cần `evidence/ip10-trace.json` |

### Evidence đã sinh được

`evidence/`: `ip05-qdrant-search.json`, `ip06-mlflow-release.json`,
`ip07-vllm-identity.json` (âm tính trung thực: `is_real_vllm: false`),
`integration-report.json`.

`ip03-delta-history.json` **thất bại có lý do**: `LakehouseUnavailable: no Delta
table`. 6 file còn lại chỉ có thể do integration test sinh ra, và toàn bộ
integration-tests bị chặn ở session guard (`integration-tests/conftest.py:117-158`)
vì guard đòi Airflow `/api/v2/monitor/health` — Airflow chỉ có ở profile full.
Đó là lý do kỹ thuật, không phải lựa chọn.

### Quan sát trực tiếp (không phải file evidence chuẩn)

- Jaeger `/api/services` → `["lab28-api","lab28-gateway"]`: trace đang chảy qua
  cả gateway và API, tức `traceparent` không bị đứt ở biên IP08/IP10.
- Prometheus `/api/v1/targets` → **9 up, 1 down** (target down là vLLM).

### IP06 — promote và rollback không sửa mã

```
champion = v2
lab28 rollback   → "champion moved from v2 to v1"
champion = v1
lab28 release    → "registered lab28-rag-release v3 as champion"
champion = v3
```

Toàn bộ bằng alias, không build lại image, không sửa dòng code nào.

## 3. Giải thích bốn quyết định kỹ thuật

### IP01 + IP10 — `event_headers`

`idempotency-key` luôn gửi; `traceparent` chỉ gửi khi có trace thật. Bỏ hẳn
header thay vì gửi chuỗi rỗng, vì `traceparent` rỗng không khớp regex W3C
(`contracts.py:205-208`) và consumer sẽ coi là trace hỏng — đứt IP10 ở đúng chỗ
khó debug nhất. Trả về `list` vì producer còn `append` `schema_version`
(`event_bus.py:181`).

### IP03 — `dedupe_latest`

Delta MERGE fail hẳn khi hai dòng source khớp cùng một dòng target, nên batch
phải unique theo `idempotency_key` trước writer. Thứ tự thắng
`(occurred_at, event_id)`: timestamp quyết định "mới nhất", `event_id` phá thế
hoà để kết quả phụ thuộc nội dung batch chứ không phụ thuộc thứ tự Kafka giao
partition. Sort theo key để hai lần chạy cho cùng một MERGE source.

Trade-off: dedupe trong Python, một pass, giữ batch trong RAM. Ở production nên
là window function trong Spark (`row_number() over (partition by key order by
occurred_at desc, event_id desc)`).

### IP04 — `feast_online_request`

Feature list lấy từ `FEATURE_REFS` (`contracts.py:400-405`); viết lại danh sách
sẽ làm feature view và serving path trôi khỏi nhau, chỉ lộ ra khi Feast trả
`NOT_FOUND` giữa lúc demo. `full_feature_names=false` để key trả về là
`avg_rating`, khớp parser `feature_store.py:180-192`. Join key là `asker_id` theo
`feature-repo/definitions.py:6` — chú ý tên entity (`asker`) khác join key.

### IP07 + IP08 — `readiness_status`

Mandatory fail → `not_ready` (return ngay); chỉ optional fail → `degraded`; còn
lại → `ready`. Đọc iterable một lần vì `readiness.py:241-243` truyền generator.
Bằng chứng vận hành ở mục 2.

## 4. Vấn đề gặp thật khi chạy và cách xử lý

1. **Hết dung lượng ổ C:.** Đo thật: image cần cho profile cơ bản ~8.8 GB +
   build cache 2.9 GB. Riêng `vllm/vllm-openai:v0.28.0` là **8,234 MB nén**
   (~18–22 GB sau khi giải nén) — đây mới là thứ làm tràn ổ, không phải phần còn
   lại của lab. Đã thu hồi **11 GB** từ cache không liên quan (`.gradle`,
   `uv cache prune`, `pip cache purge`, npm cache, `%TEMP%`): C: từ 19.1 → 30.1 GB.
2. **Không có `.dockerignore`.** Build context gồm cả `.venv` 0.77 GB, bị nạp vào
   BuildKit cache mỗi lần build. Đã thêm `.dockerignore`; 4 kiểm tra tĩnh vẫn pass.
   `docker builder prune -f` thu hồi thêm 1.76 GB.
3. **`lab28 release` crash trên Windows.** MLflow client in emoji `🏃` ra stdout,
   console cp1252 → `UnicodeEncodeError`. Không phải lỗi lab; xử lý bằng
   `PYTHONUTF8=1` (và `PYTHONIOENCODING=utf-8`) cho process. Lần crash đó vẫn tạo
   version 1 trong registry, nên registry có sẵn version cũ để demo rollback.
4. **`lab28 seed --via-gateway` trả exit 1 với 5 × HTTP 429 `local_rate_limited`.**
   Envoy cấu hình `max_tokens: 10`, `tokens_per_fill: 10`, `fill_interval: 1s`
   (`gateway/envoy.yaml:46-48`), còn CLI bắn 25 request tuần tự **không có pacing**
   (`cli.py:228-235`). Đây là rate limiter làm đúng việc của nó (bằng chứng cho
   IP08), không phải lỗi cấu hình. Xử lý: nạp corpus qua API trực tiếp (API vẫn là
   producer duy nhất, vẫn qua validation + idempotency + traceparent) và chứng minh
   đường gateway bằng một lần seed có pacing (`--limit 3`, 0 rejected).
   → Nhận xét: README Bước 7 dùng đúng một lệnh `seed --via-gateway` cho toàn bộ
   corpus, lệnh này sẽ luôn trả exit 1 với ngưỡng 10 rps mặc định.

## 5. Production gaps

1. **Không có xác thực giữa các service.** Kafka, Qdrant, MLflow, Feast mở trong
   compose network; `LAB28_QDRANT_API_KEY`/`LAB28_VLLM_API_KEY` mặc định rỗng.
   Production cần mTLS + SASL.
2. **`replication_factor=1` cho mọi topic** (`contracts.py:87-121`): mất broker là
   mất dữ liệu. Tối thiểu RF=3, `min.insync.replicas=2`.
3. **Dedupe chỉ trong phạm vi một batch**; cùng key ở hai batch khác nhau thì
   tính idempotent phụ thuộc hoàn toàn vào `MERGE ... WHEN MATCHED THEN UPDATE`.
4. **Không có schema registry**: `schema_version` được gửi nhưng không ai từ chối
   phiên bản lạ ở runtime.
5. **`/ready` không có hysteresis**: dependency nhấp nháy sẽ làm instance flapping
   vào/ra rotation.
6. **Rate limit là local per-Envoy**, không phải global — nhiều replica sẽ nhân
   ngưỡng lên theo số replica.
7. **GitOps chỉ kiểm tra tĩnh**: `gitops/application.yaml` ghim
   `refs/tags/v3.0.0` nhưng không có cluster Argo CD để chứng minh drift/self-heal.
8. **Ingest không có backpressure**: API nhận 202 rồi publish; nếu Kafka chậm,
   không có hàng đợi phía client nào ngoài DLQ.

## 6. Bằng chứng còn thiếu và điều kiện lấy được

| Deliverable (`SUBMISSION.md`) | Trạng thái | Điều kiện |
|---|---|---|
| `integration-report.json` | **có** (score 67) | — |
| Fast suite output | **có** (87 passed) | — |
| `evidence/ip05`, `ip06`, `ip07` | **có** | — |
| `evidence/ip03-delta-history.json` | thiếu | Spark Connect (profile full) |
| `ip01`, `ip02`, `ip04`, `ip08`, `ip09` ×2, `ip10` | thiếu | integration-tests, bị chặn bởi session guard đòi Airflow |
| Happy-path trace (run/trace/Delta/MLflow version) | thiếu | J1 (profile full) |
| Failure/recovery + no-data-loss | thiếu | J4 (profile full) |
| Load profile P50/P95/P99 | thiếu | cần `/api/v1/ask` trả lời được, tức cần vLLM |
| K8s/GitOps drift/rollback | tĩnh pass; drift/rollback thiếu | cần cluster Argo CD |

Để lấy nốt: `docker compose --env-file ports.template --profile full up -d
spark-connect airflow --wait` (thêm ~4.5–5.5 GB), chạy J1/J2, rồi
`docker image rm apache/spark:...` + `docker builder prune -af` để trả lại chỗ.

## 7. Đóng góp

Làm cá nhân: Trần Đình Đăng (`2A202601998`) thực hiện toàn bộ Bước 1–7 (4 hàm
integration, fast suite, các kiểm tra tĩnh, dựng và chạy hệ thống cơ bản 12
service, sinh evidence, demo promote/rollback MLflow) và viết tài liệu này. Không
có thành viên nào khác tham gia.

## 8. Reflection

- **Học được gì rõ nhất:** `ready`/`degraded`/`not_ready` không phải ba mức độ lỗi
  mà là ba **hành động khác nhau của load balancer**. Thấy rõ nhất khi `/ready`
  trả `degraded` + HTTP 200 trong lúc vLLM đang chết.
- **Chỗ dễ sai nhất:** gửi `traceparent` rỗng thay vì bỏ header, và dedupe không
  phá thế hoà — cả hai pass "happy path" rồi hỏng khi replay.
- **Bất ngờ nhất:** ba lỗi tốn thời gian nhất đều không nằm trong bốn hàm phải
  làm: dung lượng ổ đĩa, encoding console Windows, và rate limiter của gateway
  chặn chính lệnh seed mà README đề nghị.
- **Nếu làm lại:** kiểm tra dung lượng ổ và `docker system df` **trước** khi viết
  code, vì 40/100 điểm nằm ở live evidence của 10 IP chứ không ở bốn hàm.
