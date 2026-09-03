# ANSWERS.md — Day 28 Track 2

- **Sinh viên:** Trần Đình Đăng — `2A202601998`
- **Hình thức:** cá nhân
- **Nhánh làm việc:** `ca-nhan-dang` (PR vào `main`)
- **Máy chạy:** Windows 11, 12 vCPU, 15.7 GiB RAM, ~19 GB trống, NVIDIA RTX 4050 Laptop (6 GiB VRAM)
- **Profile preflight:** `browser-fallback` (Docker CLI có, `docker_daemon: false`)

> Trạng thái trung thực: bài này **chưa chạy Docker**, nên mọi bằng chứng cần hệ
> thống sống (IP01–IP10 live evidence, 5 journeys, load profile, Grafana/Jaeger)
> đều được ghi là `UNVERIFIED`. Không có số liệu nào trong tài liệu này được bịa
> hoặc mô phỏng, theo `docs/rubric.md:12-13` và `SUBMISSION.md:23-24`.

---

## 1. Phần đã hoàn thành và kiểm chứng được (Bước 1–6)

Bốn ranh giới trong `src/lab28_platform/integration_tasks.py` đã hoàn thiện, và
được gọi trực tiếp bởi luồng chạy thật:

| Hàm | IP | Call site thật |
|---|---|---|
| `event_headers` | IP01 + IP10 | `src/lab28_platform/event_bus.py:178` |
| `dedupe_latest` | IP03 | `src/lab28_platform/delta_store.py:206` |
| `feast_online_request` | IP04 | `src/lab28_platform/feature_store.py:104` |
| `readiness_status` | IP07 + IP08 | `src/lab28_platform/readiness.py:241` |

Kết quả kiểm tra (đã chạy trên máy, exit code thật):

| Lệnh | Kết quả |
|---|---|
| `uv run pytest starter-tests -q` (trước khi sửa) | 4 failed — đúng baseline `NotImplementedError` |
| `uv run pytest starter-tests tests -q` | **87 passed** |
| `uv run pytest tests/test_delta_merge_idempotency.py -q` | 22 passed |
| `uv run ruff check .` | All checks passed (exit 0) |
| `uv run python scripts/verify_matrix.py` | 245 checks passed (exit 0) |
| `uv run python scripts/check_portability.py` | OK (exit 0) |
| `uv run python scripts/validate_manifests.py` | passed (exit 0) |
| `docker compose --env-file ports.template config --quiet` | exit 0 |
| `docker compose --env-file ports.template --profile full config --quiet` | exit 0 |
| `docker compose -f compose.yaml -f compose.gpu.yaml --profile gpu config --quiet` | exit 0 |

Không có cổng mặc định nào của lab (3000, 4040, 4317/4318, 5000, 6333, 6566,
6570, 8000, 8001, 8080, 8082, 9090, 9091, 9092, 9901, 15002, 16686) đang bị
chiếm trên máy này, nên `ports.template` dùng được nguyên bản.

## 2. Giải thích bốn quyết định kỹ thuật

### IP01 + IP10 — `event_headers`

`idempotency-key` luôn được gửi; `traceparent` chỉ được gửi khi có trace thật.
Điểm quan trọng là **bỏ hẳn header** thay vì gửi chuỗi rỗng: `traceparent` rỗng
không khớp regex W3C (`contracts.py:205-208`), consumer sẽ coi là trace hỏng và
làm đứt chuỗi IP10 ở đúng chỗ khó debug nhất. Hàm trả về `list` (không phải
tuple) vì producer còn `append` thêm `schema_version` (`event_bus.py:181`).

Trade-off: header là bytes thô, không có schema. Đổi tên header là breaking
change không được kiểm tra bởi bất cứ validator nào — nên tên header được đặt
thành hằng số ở một chỗ duy nhất.

### IP03 — `dedupe_latest`

Delta MERGE **fail hẳn** khi hai dòng source khớp cùng một dòng target, nên batch
phải unique theo `idempotency_key` *trước khi* tới writer. Thứ tự thắng là
`(occurred_at, event_id)`: `occurred_at` quyết định "bản mới nhất", `event_id`
phá thế hoà để kết quả phụ thuộc **nội dung batch**, không phụ thuộc thứ tự Kafka
giao partition. Kết quả sort theo key nên hai lần chạy cùng batch cho cùng một
MERGE source (quan trọng khi so sánh Delta history giữa các lần replay).

Trade-off: dedupe trong Python, một pass, giữ toàn bộ batch trong RAM. Đúng cho
batch cỡ lab; ở production nên đẩy thành window function trong Spark
(`row_number() over (partition by key order by occurred_at desc, event_id desc)`)
để không bị giới hạn bởi RAM của driver.

### IP04 — `feast_online_request`

Danh sách feature lấy từ `FEATURE_REFS` (`contracts.py:400-405`) thay vì viết lại
— nếu viết lại, feature view và serving path sẽ trôi khỏi nhau một cách im lặng
và chỉ lộ ra khi Feast trả `NOT_FOUND` trong lúc demo. `full_feature_names=false`
để key trả về là `avg_rating` chứ không phải `asker_activity_v1__avg_rating`,
khớp với parser ở `feature_store.py:180-192`. Entity join key là `asker_id` theo
`feature-repo/definitions.py:6` (`Entity(name="asker", join_keys=["asker_id"])`)
— chú ý tên entity (`asker`) khác join key (`asker_id`).

### IP07 + IP08 — `readiness_status`

Thứ tự ưu tiên: một probe `mandatory` fail → `not_ready` (return ngay, vì lỗi bắt
buộc áp đảo); chỉ probe không bắt buộc fail → `degraded`; còn lại → `ready`.
Ý nghĩa vận hành: `not_ready` là tín hiệu để Envoy **rút instance khỏi rotation**
(`contracts.py:146-150`), còn `degraded` là "vẫn trả lời được nhưng thiếu một
phần" — nếu gộp hai trạng thái này thành một, ta sẽ hoặc mất traffic một cách
không cần thiết, hoặc phục vụ câu trả lời không có grounding mà load balancer
không hề biết. Hàm chỉ đọc iterable một lần vì `readiness.py:241-243` truyền vào
một generator.

## 3. Trade-offs khác đã cân nhắc

- **`--no-editable` + `uv sync --frozen`**: đánh đổi tốc độ rebuild lấy việc loại
  bỏ khác biệt filesystem/permission giữa Windows/macOS/Linux. Trên máy này `uv`
  cảnh báo không hardlink được (cache khác ổ đĩa với repo) và fallback sang copy —
  chậm hơn nhưng không ảnh hưởng tính đúng đắn.
- **Dedupe ở Python thay vì trong Spark job**: kiểm thử được không cần JVM
  (`tests/test_delta_merge_idempotency.py` chạy trong 0.56s), đổi lại rule này
  nằm ngoài engine nên phải tự giữ đồng bộ với `merge_sql`.
- **Nhánh riêng thay vì commit trực tiếp lên `main`**: `.github/workflows/ci.yml:26`
  chạy `scripts/verify_starter_state.py` khi push vào `main`, và script này yêu
  cầu **đúng 4 `NotImplementedError`**. Push bài đã làm xong lên `main` sẽ fail CI
  theo thiết kế; chỉ nhánh + pull_request là đường đúng.

## 4. Production gaps (những chỗ lab này chưa đạt mức production)

1. **Không có xác thực giữa các service.** Kafka, Qdrant, MLflow, Feast, Spark
   Connect đều mở trong compose network; `LAB28_QDRANT_API_KEY` và
   `LAB28_VLLM_API_KEY` tồn tại nhưng mặc định rỗng. Production cần mTLS + SASL.
2. **`replication_factor=1` cho mọi topic** (`contracts.py:87-121`): mất broker là
   mất dữ liệu. Production tối thiểu RF=3 với `min.insync.replicas=2`.
3. **Dedupe chỉ trong phạm vi một batch.** Nếu cùng `idempotency_key` tới ở hai
   batch khác nhau, tính idempotent phụ thuộc hoàn toàn vào `MERGE ... WHEN
   MATCHED THEN UPDATE`, không còn `dedupe_latest` bảo vệ.
4. **Không có kiểm soát schema evolution ở runtime.** `schema_version` được gửi
   kèm nhưng không có schema registry nào từ chối phiên bản lạ.
5. **GitOps chỉ được kiểm tra tĩnh.** `gitops/application.yaml` ghim
   `targetRevision: refs/tags/v3.0.0` và trỏ về repo gốc VinUni; không có cluster
   Argo CD nào để chứng minh drift detection / self-heal.
6. **`/ready` là bản ảnh tại một thời điểm**, không có hysteresis: một dependency
   nhấp nháy sẽ làm instance vào/ra rotation liên tục (flapping).
7. **Không có budget enforcement thật.** `LAB28_BUDGET_*` chỉ để quan sát, không
   cắt request khi vượt ngưỡng.

## 5. Trạng thái từng integration point

| IP | Ranh giới | Trạng thái | Cơ sở |
|---|---|---|---|
| IP01 | ingestion → Kafka | **implemented**, live `UNVERIFIED` | `event_headers` pass; cần Kafka để consume thật |
| IP02 | Kafka → Airflow | `UNVERIFIED` | cần `--profile full` |
| IP03 | pipeline → Delta | **implemented**, live `UNVERIFIED` | 22/22 test merge idempotency pass |
| IP04 | Delta → Feast | **implemented**, live `UNVERIFIED` | request khớp registry |
| IP05 | data → Qdrant | `UNVERIFIED` | cần `lab28 index` |
| IP06 | MLflow registry | `UNVERIFIED` | cần `lab28 release` |
| IP07 | model → vLLM | `UNVERIFIED` (gate GPU, chủ động bỏ) | không cấu hình endpoint; không giả lập |
| IP08 | serving → gateway | **implemented** (readiness semantics), live `UNVERIFIED` | cần Envoy |
| IP09 | → Prometheus/Grafana | `UNVERIFIED` | cần stack |
| IP10 | → LangSmith/tracing | **implemented** (traceparent), leg LangSmith `UNVERIFIED` | cần stack + exporter thứ hai |

Ghi chú IP10: test LangSmith (`integration-tests/test_trace_span_coverage.py:150`)
đòi Prometheus thấy **≥2 span exporter**, nhưng
`monitoring/otel-collector.yaml:16-22` chỉ khai báo `otlp/jaeger` + `debug`. Muốn
đạt leg này phải thêm exporter LangSmith vào collector, không chỉ là có API key.

## 6. Bằng chứng còn thiếu và điều kiện để lấy được

| Deliverable (`SUBMISSION.md`) | Trạng thái | Điều kiện |
|---|---|---|
| `integration-report.json` | thiếu | `lab28 integration` cần stack chạy (exit 1 nếu not_ready) |
| 11 file `evidence/*` | thiếu | Bước 7–8 |
| Happy-path trace (run ID, trace ID, Delta version, MLflow version) | thiếu | J1 |
| Failure/recovery + no-data-loss | thiếu | J4 |
| Load profile P50/P95/P99 | thiếu | `load-tests/run_profile.py --requests 200 --workers 8` cần API |
| K8s/GitOps drift/rollback | **tĩnh: pass**; drift/rollback thiếu | cần cluster Argo CD |
| Fast suite output | **có** (87 passed) | — |

Để lấy nốt: bật Docker Desktop → `docker compose --env-file ports.template up -d
--build --wait` (Bước 7) → `--profile full` (Bước 8). Cần lưu ý ổ C: chỉ còn
~19 GB, riêng image `vllm/vllm-openai` đã ~10 GB.

## 7. Đóng góp

Làm cá nhân: Trần Đình Đăng (`2A202601998`) thực hiện toàn bộ Bước 1–6 (4 hàm
integration, fast suite, ruff, verify_matrix, check_portability,
validate_manifests, compose config cho cả ba profile) và viết tài liệu này.
Không có thành viên nào khác tham gia.

## 8. Reflection

- **Học được gì rõ nhất:** ba trạng thái `ready`/`degraded`/`not_ready` không
  phải ba mức "mức độ lỗi" mà là ba **hành động khác nhau của load balancer**.
- **Chỗ dễ sai nhất:** gửi `traceparent` rỗng thay vì bỏ header, và dedupe không
  phá thế hoà — cả hai đều pass "happy path" rồi hỏng khi replay.
- **Nếu làm lại:** dựng Docker trước khi viết code, vì phần chấm điểm lớn nhất
  (40/100 cho live evidence của 10 IP) hoàn toàn nằm ở hệ thống chạy thật, không
  nằm ở bốn hàm.
