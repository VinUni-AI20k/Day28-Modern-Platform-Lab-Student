# PLAN — Day 28 Track 2 (Trương Văn Thái, 2A202601801)

Kế hoạch thực hiện lab "Platform Integration & Production Readiness".
Nguồn: [README.md](README.md), [LAB28.md](LAB28.md), [LAB28_GUIDE.md](LAB28_GUIDE.md),
[SUBMISSION.md](SUBMISSION.md), [docs/rubric.md](docs/rubric.md),
[contracts/integration-matrix.yaml](contracts/integration-matrix.yaml).

---

## 0. Hiện trạng đã kiểm chứng (2026-09-03)

| Hạng mục | Kết quả | Ảnh hưởng |
|---|---|---|
| Repo | Đã clone, branch `main` → `origin/main`, working tree sạch | Bỏ qua `git clone`, chỉ cần tạo branch cá nhân |
| `uv` | 0.11.17 ✓ | Sẵn sàng |
| `.venv` | **Chưa có** | Phải `uv sync` trước mọi lệnh |
| Docker | CLI ✓, Compose v5.1.0 ✓, **daemon chưa chạy** | `preflight` hiện trả `browser-fallback` |
| Phần cứng | 12 CPU, **7.7 GiB RAM**, 105 GB trống ổ D: | Core stack: tạm được. Profile `full`: rủi ro cao |
| 4 hàm bài tập | Cả 4 còn `NotImplementedError` | Đây là điểm bắt đầu đúng |
| `.lab28/`, `evidence/`, `ANSWERS.md` | Chưa có | Sinh ra ở Phase 3 và Phase 6 |

### Hai ràng buộc phải nhớ

1. **RAM là nút thắt.** Core stack = 13 container (`runtime-init`, `kafka`,
   `kafka-exporter`, `qdrant`, `mlflow`, `feast`, `api`, `gateway`, `jaeger`,
   `otel-collector`, `pushgateway`, `prometheus`, `grafana`). Profile `full`
   thêm `spark-connect` (Spark 4.2 JVM) và `airflow` — README khuyến nghị
   12–16 GB. Với 7.7 GB, **Phase 4 nhiều khả năng phải chạy trên máy chung**.

2. **`preflight` báo RAM sai trên Windows.** `run_preflight` đọc RAM bằng
   `os.sysconf` (`src/lab28_platform/readiness.py:376`) — hàm này không tồn tại
   trên Windows, nên `memory_gib = null` và bị bỏ khỏi công thức tính profile.
   Ngay khi bật Docker, preflight sẽ báo `local-standard` **dù RAM thật chỉ
   7.7 GB**. Không dùng kết quả đó để kết luận máy chạy nổi profile `full`.

---

## Phase 0 — Chuẩn bị môi trường (~10 phút, không cần Docker)

```text
git switch -c ca-nhan-thai
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run lab28 --help
uv run lab28 preflight
uv run pytest starter-tests -q
```

**Đạt khi:**

- [ ] `git status` hiển thị branch `ca-nhan-thai`, chưa có file bị sửa.
- [ ] `lab28 --help` liệt kê `preflight`, `topics`, `seed`, `ready`.
- [ ] `preflight` in `profile`, `python=3.11.x`, `docker_daemon`, `memory_gib`, `next`.
- [ ] `pytest starter-tests -q` → **đúng 4 failed**, tất cả là `NotImplementedError`.

Lưu output của lần chạy 4-fail này (baseline "trước khi làm") — dùng khi trình bày.

---

## Phase 1 — Hoàn thiện 4 hàm (~1–2 giờ, không cần Docker)

Chỉ sửa **một file**: `src/lab28_platform/integration_tasks.py`.
Không sửa test, không bọc `try/except` để giấu lỗi. Cả 4 hàm đều được luồng chạy
thật gọi trực tiếp, nên code viết ở đây chính là code chạy trong demo.

| Hàm | IP | Nơi hệ thống thật gọi |
|---|---|---|
| `event_headers` | IP01 + IP10 | `src/lab28_platform/event_bus.py:178` |
| `dedupe_latest` | IP03 | `src/lab28_platform/delta_store.py:206` |
| `feast_online_request` | IP04 | `src/lab28_platform/feature_store.py:104` |
| `readiness_status` | IP07 + IP08 | `src/lab28_platform/readiness.py:241` |

### A. `event_headers` — header đi kèm bản tin Kafka

Yêu cầu:

- luôn có `("idempotency-key", <bytes>)`;
- có traceparent → thêm `("traceparent", <bytes>)`; không có → **bỏ hẳn mục này**,
  không gửi chuỗi rỗng (header W3C rỗng là header không hợp lệ);
- không hard-code key hay traceparent.

Bẫy: caller ở `event_bus.py:181` gọi `headers.append(("schema_version", ...))`
ngay sau đó → **phải trả về `list` mutable**, trả `tuple` sẽ vỡ luồng thật dù
starter-test vẫn xanh.

```text
uv run pytest starter-tests/test_integration_tasks.py -k event_headers -q
```
- [ ] `1 passed, 3 deselected`

### B. `dedupe_latest` — chống trùng khi Kafka replay

Yêu cầu:

- duyệt iterable đầu vào **đúng một lần** (`list(events)` ngay đầu hàm);
- một bản ghi cho mỗi `idempotency_key`;
- bản thắng là bản có `(occurred_at, event_id)` lớn nhất;
- kết quả sắp theo `idempotency_key` để mọi lần chạy cho cùng thứ tự;
- input rỗng → `[]`.

Bẫy: `tests/test_delta_merge_idempotency.py` khó hơn starter-test. Nó tạo
`IngestionEvent` **không truyền `event_id`** (auto `uuid4().hex`,
`contracts.py:304`), nên:

- `test_events_sharing_a_timestamp_resolve_deterministically` bắt buộc phải
  tie-break bằng `event_id`;
- `test_the_result_does_not_depend_on_delivery_order` loại bỏ mọi lời giải kiểu
  "bản đến sau thắng".

```text
uv run pytest starter-tests/test_integration_tasks.py -k delta_source -q
uv run pytest tests/test_delta_merge_idempotency.py -q
```
- [ ] Cả hai lệnh đều xanh. Nếu lệnh đầu xanh mà lệnh sau đỏ → chưa xử lý đúng
      đối tượng `IngestionEvent`.

### C. `feast_online_request` — hợp đồng với Feast

Yêu cầu:

- `entities = {"asker_id": [asker_id]}`;
- `features` = 4 feature của `asker_activity_v1`;
- `full_feature_names = False`;
- lấy danh sách từ `FEATURE_REFS` trong `src/lab28_platform/contracts.py:400`
  (`list(FEATURE_REFS)`), **không chép tay 4 chuỗi** — đây chính là tiêu chí
  "không viết lại cùng một danh sách ở nhiều nơi".

```text
uv run pytest starter-tests/test_integration_tasks.py -k feast_request -q
```
- [ ] `1 passed, 3 deselected`

### D. `readiness_status` — ready / degraded / not_ready

Thứ tự ưu tiên:

1. có ít nhất một probe `mandatory=True` và `ready=False` → `not_ready`;
2. không lỗi bắt buộc nhưng có probe `ready=False` → `degraded`;
3. còn lại → `ready`.

Bẫy: `readiness.py:241` truyền vào một **generator**, không phải list → duyệt
một lần rồi mới kết luận (materialize hoặc gom cờ trong một vòng lặp).

```text
uv run pytest starter-tests/test_integration_tasks.py -k readiness -q
```
- [ ] `1 passed, 3 deselected`

### Cổng chặn cuối Phase 1

```text
uv run pytest starter-tests tests -q
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
```

- [ ] Không còn `NotImplementedError`;
- [ ] cả 5 lệnh exit code `0`;
- [ ] commit mốc 1: `feat: implement four integration boundaries (IP01/03/04/07-08)`.

**Chưa đạt thì chưa động tới Docker.**

---

## Phase 2 — Kiểm tra cấu hình Compose (~5 phút)

```text
docker compose --env-file ports.template config --quiet
docker compose --env-file ports.template --profile full config --quiet
```

Cổng cần trống: 8080 (gateway), 8000 (API), 3000 (Grafana), 9090 (Prometheus),
5000 (MLflow), 8001 (vLLM), 8082 (Airflow), 16686 (Jaeger), 6333 (Qdrant).

- [ ] Cả hai lệnh im lặng và trả `0`.
- [ ] Nếu trùng cổng: copy `ports.template` → `ports.local`, đổi **chỉ số cổng**,
      rồi dùng `--env-file ports.local` cho *mọi* lệnh sau đó. Không đưa token/
      mật khẩu/URL bí mật vào file này.

---

## Phase 3 — Core stack (~1–2 giờ, cần Docker)

Chuẩn bị trước khi `up`:

1. Bật Docker Desktop, đợi `docker info` chạy được.
2. Với 7.7 GB RAM: tạo `%USERPROFILE%\.wslconfig` cấp ~5–6 GB cho WSL2, chạy
   `wsl --shutdown`, mở lại Docker Desktop. Đóng bớt trình duyệt/IDE nặng.
3. `uv run lab28 preflight` lại (nhớ cảnh báo về `memory_gib` ở mục 0).

```text
docker compose --env-file ports.template up -d --build --wait
docker compose --env-file ports.template ps
uv run lab28 topics
uv run lab28 index --source file
uv run lab28 release
uv run lab28 seed --via-gateway
uv run lab28 inspect
uv run lab28 ready
```

**Đạt khi:**

- [ ] `ps`: các service `running`/`healthy`;
- [ ] `topics`: topic `created` hoặc `exists`;
- [ ] `index`: `points_upserted > 0`;
- [ ] `release`: có MLflow version + alias `champion`;
- [ ] `seed`: documents/feedback `accepted`, không có `rejected`;
- [ ] `ready`: `ready` **hoặc** `degraded`. `not_ready` phải điều tra bằng
      `lab28 ready` → tìm component lỗi → `docker compose ... logs <service>`.

`degraded` vì chưa nối vLLM thật là **trạng thái đã dự kiến**, không phải lý do
để dựng server vLLM giả (rubric: làm giả = 0 điểm phần đó).

### Thu bằng chứng UI ngay tại phase này

| UI | URL | Chứng minh |
|---|---|---|
| Envoy | http://localhost:8080/health | IP08 định tuyến |
| API docs | http://localhost:8000/docs | hợp đồng HTTP |
| Grafana | http://localhost:3000 | IP09 golden signals |
| Prometheus | http://localhost:9090/targets | IP09 targets up |
| Jaeger | http://localhost:16686 | IP10 một trace xuyên hệ thống |
| MLflow | http://localhost:5000 | IP06 champion |
| Qdrant | http://localhost:6333/dashboard | IP05 points > 0 |

- [ ] Commit mốc 2 (chỉ code/cấu hình, **không commit** `.lab28/`, DB, cache, weights).

---

## Phase 4 — Full data/ML: J1–J5 (rủi ro cao trên máy này)

```text
docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 seed --via-gateway
uv run pytest integration-tests/test_j1_golden_path.py -q
uv run pytest integration-tests/test_j2_idempotent_replay.py -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

Airflow ở http://localhost:8082, DAG `lab28_ingestion_pipeline` — đối chiếu log
từng task với Delta, Feast, Qdrant, MLflow.

| Journey | Chứng minh |
|---|---|
| J1 golden path | API → Kafka → Airflow → Delta → Feast/Qdrant → response |
| J2 replay | gửi lại cùng lô, số bản ghi **không tăng** |
| J3 promotion/rollback | đổi champion rồi quay lại version cũ, không sửa code |
| J4 degraded/recovery | dừng 1 dependency không bắt buộc → `degraded` → khôi phục |
| J5 trace/metrics | trace ID và metric liên tục xuyên luồng |

**Chiến lược rủi ro:** thử một lần. Nếu container OOM/`unhealthy` do RAM thì
**không ép** — dừng stack, ghi lại triệu chứng, chuyển Phase 4 sang máy chung /
hạ tầng giảng viên. README cho phép đúng đường này: Bước 1–6 tại máy cá nhân,
Bước 7–9 trên hệ thống chung.

---

## Phase 5 — vLLM thật cho IP07 (tùy chọn, cần GPU)

Theo [KAGGLE_GPU_EXTENSION.md](KAGGLE_GPU_EXTENSION.md): Kaggle **T4** (không dùng
P100), `vllm==0.26.0`, model `Qwen/Qwen3-4B-Instruct-2507`.

Bốn bằng chứng bắt buộc — thiếu một cái là **không đạt IP07**:

- [ ] `/version` báo đúng build vLLM;
- [ ] `/v1/models` chứa model ID đã cấu hình;
- [ ] `/metrics` có series bắt đầu bằng `vllm:`;
- [ ] request từ hệ thống trả về trace ID + tên model + version để đối chiếu.

Không commit URL tunnel hay token vào Git/notebook. Ghi vào ADR các giới hạn:
quota session, rủi ro tunnel public, cold start do tải model, tensor parallel.

---

## Phase 6 — Evidence, load test, demo, nộp bài

```text
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
uv run python load-tests/run_profile.py --requests 200 --workers 16
```

Ghi kèm số đo: P50/P95/P99, CPU/RAM của API, Kafka lag, error rate, **cấu hình
phần cứng, model, dataset, concurrency, warm-up** (theo `runbooks/performance.md`).
Không suy ra capacity production từ laptop.

### Bản đồ 10 điểm kết nối → file bằng chứng

| IP | Boundary | File evidence |
|---|---|---|
| IP01 | HTTP → Kafka | `evidence/ip01-kafka-consume.json` |
| IP02 | Kafka → Airflow | `evidence/ip02-airflow-run.json` |
| IP03 | Pipeline → Delta | `evidence/ip03-delta-history.json` |
| IP04 | Delta → Feast | `evidence/ip04-feast-online.json` |
| IP05 | Delta → Qdrant | `evidence/ip05-qdrant-search.json` |
| IP06 | Eval → MLflow Registry | `evidence/ip06-mlflow-release.json` |
| IP07 | RAG → vLLM thật *(gate: gpu)* | `evidence/ip07-vllm-identity.json` |
| IP08 | Client → Envoy | `evidence/ip08-gateway.json` |
| IP09 | → Prometheus/Grafana | `evidence/ip09-prometheus-targets.json`, `evidence/ip09-grafana-dashboards.json` |
| IP10 | → OTLP trace *(gate: langsmith)* | `evidence/ip10-trace.json` |

### Danh sách nộp (SUBMISSION.md)

- [ ] `integration-report.json` + output fast suite;
- [ ] 10 file evidence đúng tên ở bảng trên;
- [ ] sơ đồ kiến trúc + phân công;
- [ ] happy-path trace có run ID, trace ID, Delta version, MLflow version;
- [ ] hồ sơ failure/recovery + chứng minh không mất dữ liệu;
- [ ] load profile P50/P95/P99 + phân tích nút cổ chai;
- [ ] validate K8s/GitOps + bằng chứng drift/rollback;
- [ ] `ANSWERS.md`: trade-off, khoảng cách so với production, đóng góp cá nhân.

Lệnh xác nhận trước khi nộp:

```text
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
uv run pytest tests -q
uv run pytest integration-tests -m "not gpu and not langsmith" -q
```

Phần nào không có hạ tầng (GPU / LangSmith) → khai `UNVERIFIED` và nộp evidence
local tương ứng. Khai trung thực vẫn được chấm; làm giả bị 0 điểm phần đó.

### Checklist demo (README Bước 10 + `docs/demo-runbook.md`)

- [ ] Sơ đồ kiến trúc, người phụ trách, 10 điểm kết nối;
- [ ] Happy path có run ID / trace ID / Delta version / MLflow version;
- [ ] Kafka replay nhưng Delta không sinh bản ghi trùng;
- [ ] Một sự cố: dự đoán dấu hiệu → quan sát → khôi phục → chứng minh không mất dữ liệu;
- [ ] Golden signals trên Grafana + một trace Jaeger xuyên hệ thống;
- [ ] MLflow promote rồi rollback mà không sửa code;
- [ ] Giải thích rõ `ready` / `degraded` / `not_ready`;
- [ ] K8s/GitOps manifest hợp lệ + cách rollback;
- [ ] Tự giải thích được mọi lựa chọn kỹ thuật;
- [ ] Không có secret/DB/cache/weights trong Git.

---

## Bẫy kỹ thuật đã lường trước

| Bẫy | Dấu hiệu | Xử lý |
|---|---|---|
| `--no-editable` làm CLI dùng bản cũ | Sửa code, `pytest` xanh nhưng `uv run lab28` hành xử như cũ | Chạy lại `uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable`. (`pytest` đọc thẳng `src/` nhờ `pythonpath = ["src"]` trong `pyproject.toml:100`, CLI thì không) |
| `preflight` báo RAM `null` trên Windows | `memory_gib: null` nhưng vẫn `local-standard` | Tự đánh giá bằng RAM thật (7.7 GB) trước khi chạy `--profile full` |
| `event_headers` trả tuple | Starter-test xanh, luồng Kafka thật vỡ ở `event_bus.py:181` | Trả `list` |
| `readiness_status` duyệt generator 2 lần | Starter-test xanh (list), `/ready` thật sai | Materialize một lần |
| `dedupe_latest` không tie-break | `tests/test_delta_merge_idempotency.py` đỏ | So sánh `(occurred_at, event_id)` |
| Port đã bị chiếm | `port is already allocated` | `ports.local` + `--env-file ports.local` |
| Container `unhealthy` | Thiếu RAM hoặc dependency chưa sẵn sàng | `docker compose ... logs <service>`, sửa lỗi **xuất hiện đầu tiên** |
| Mất state khi demo recovery | — | Dùng `down --remove-orphans`, **tuyệt đối không** `down -v` / `lab28 reset --yes` trong lúc demo |

---

## Thứ tự ưu tiên nếu thiếu thời gian

1. **Phase 0–2** — bắt buộc, không cần Docker, quyết định phần lớn "Engineering quality"
   và toàn bộ 4 boundary do sinh viên sở hữu.
2. **Phase 3** — Core stack, mở khóa IP01, IP05, IP06, IP08, IP09, IP10 (local backend).
3. **Phase 6 (phần evidence + demo)** — rubric cho 10 điểm "Demo & explanation" và
   15 điểm "Observability"; làm được ngay sau Phase 3.
4. **Phase 4** — trên máy chung; mở khóa IP02, IP03, IP04 và 5 journey.
5. **Phase 5** — GPU, chỉ IP07.

Rubric ([docs/rubric.md](docs/rubric.md)): thiếu IP01–IP07 hoặc happy path thật →
tối đa 60 điểm. Vì vậy Phase 3 + Phase 4 quan trọng hơn Phase 5 rất nhiều.

---

## Dọn môi trường

```text
docker compose --env-file ports.template --profile full down --remove-orphans
```

Chỉ khi thật sự muốn xóa toàn bộ state:

```text
uv run lab28 reset --yes
```
