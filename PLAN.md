# PLAN — Day 28 Track 2 (Trương Văn Thái, 2A202601801)

Kế hoạch thực hiện lab "Platform Integration & Production Readiness".
Nguồn: [README.md](README.md), [LAB28.md](LAB28.md), [LAB28_GUIDE.md](LAB28_GUIDE.md),
[SUBMISSION.md](SUBMISSION.md), [docs/rubric.md](docs/rubric.md),
[contracts/integration-matrix.yaml](contracts/integration-matrix.yaml).

---

## Trạng thái thực thi (cập nhật 2026-09-03)

| Phase | Trạng thái | Ghi chú |
|---|---|---|
| 0 — Chuẩn bị môi trường | ✅ Xong | |
| 1 — 4 hàm boundary | ✅ Xong | commit `2983852` trên `ca-nhan-thai` |
| 2 — Kiểm tra Compose | ✅ Xong | |
| 3 — Core stack | ✅ Xong, đang chạy | 12 container healthy, `/ready` = `degraded` (đúng dự kiến) |
| 4 — Full profile (Airflow/Spark) | ❌ **Không khả thi trên máy này** | Thử 2 lần, cả 2 lần OOM-kill `kafka`; xem mục "Kết quả thực tế Phase 4" |
| 5 — vLLM GPU | ⏸ Chưa làm | Độc lập với Phase 4, có thể làm bất cứ lúc nào có endpoint GPU |
| 6 — Evidence/load test | 🟡 Một phần | 4/10 evidence + 2 lần load test; 6 evidence còn lại bị chặn bởi Phase 4/5 |

Core stack hiện đang chạy trên máy (branch `ca-nhan-thai`, chưa merge). Xem chi tiết nguyên nhân từng phase bên dưới.

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

- [x] `git status` hiển thị branch `ca-nhan-thai`, chưa có file bị sửa.
- [x] `lab28 --help` liệt kê `preflight`, `topics`, `seed`, `ready`.
- [x] `preflight` in `profile`, `python=3.11.x`, `docker_daemon`, `memory_gib`, `next`.
- [x] `pytest starter-tests -q` → **đúng 4 failed**, tất cả là `NotImplementedError`.

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
- [x] `1 passed, 3 deselected`

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
- [x] Cả hai lệnh đều xanh (22 test trong `test_delta_merge_idempotency.py`). Nếu
      lệnh đầu xanh mà lệnh sau đỏ → chưa xử lý đúng đối tượng `IngestionEvent`.

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
- [x] `1 passed, 3 deselected`

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
- [x] `1 passed, 3 deselected`

### Cổng chặn cuối Phase 1

```text
uv run pytest starter-tests tests -q
uv run ruff check .
uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py
uv run python scripts/validate_manifests.py
```

- [x] Không còn `NotImplementedError`;
- [x] cả 5 lệnh exit code `0` (87 test pass, ruff sạch);
- [x] commit mốc 1: `feat: implement four integration boundaries (IP01/03/04/07-08)`
      → `2983852` trên `ca-nhan-thai`.

**Chưa đạt thì chưa động tới Docker.**

---

## Phase 2 — Kiểm tra cấu hình Compose (~5 phút)

```text
docker compose --env-file ports.template config --quiet
docker compose --env-file ports.template --profile full config --quiet
```

Cổng cần trống: 8080 (gateway), 8000 (API), 3000 (Grafana), 9090 (Prometheus),
5000 (MLflow), 8001 (vLLM), 8082 (Airflow), 16686 (Jaeger), 6333 (Qdrant).

- [x] Cả hai lệnh im lặng và trả `0`.
- [x] Không trùng cổng — 9 cổng cần dùng đều trống (`Get-NetTCPConnection`), không
      cần tạo `ports.local`.

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

- [x] `ps`: các service `running`/`healthy`;
- [x] `topics`: topic `created` hoặc `exists`;
- [x] `index`: `points_upserted > 0` (13);
- [x] `release`: có MLflow version + alias `champion` (`lab28-rag-release v2`);
- [x] `seed`: documents/feedback `accepted`, không có `rejected` (sau khi gửi bù —
      xem "Kết quả thực tế" bên dưới);
- [x] `ready`: `degraded` (đúng dự kiến, thiếu vLLM).

`degraded` vì chưa nối vLLM thật là **trạng thái đã dự kiến**, không phải lý do
để dựng server vLLM giả (rubric: làm giả = 0 điểm phần đó).

### Kết quả thực tế Phase 3 — 4 vấn đề gặp phải và cách xử lý

1. **Build song song bị lỗi containerd race.** `docker compose up --build` lỗi
   `image "...-feast:latest": already exists` khi build `feast` + `api` cùng lúc.
   Xử lý: build từng service riêng (`docker compose build api`, rồi `feast`) trước
   khi `up -d --wait` (không cần `--build` nữa vì image đã sẵn).
2. **MLflow crash trên console Windows.** `lab28 release` crash với
   `UnicodeEncodeError` vì MLflow in emoji (`🏃`) mà console Windows mặc định
   dùng codepage `cp1252`. Model version 1 vẫn được tạo trước khi crash — chạy
   lại lệnh (tạo thêm version 2) với `PYTHONIOENCODING=utf-8` set trước mọi lệnh
   `uv run lab28 ...` để tránh lặp lại.
3. **`seed --via-gateway` báo `rejected` do rate-limit, không phải lỗi code.**
   Envoy giới hạn `10 tokens/s` dùng chung cho **toàn bộ route** (`gateway/envoy.yaml:45-48`),
   áp dụng cho cả 25 request (13 doc + 12 feedback) gửi gần như đồng thời. 4/12
   feedback (asker-005, asker-006) bị `429`. Đây chính là bằng chứng cần cho
   IP08 (`evidence/ip08-gateway.json` cần cả `200` và `429`), không phải bug. Đã
   gửi bù 4 bản ghi còn thiếu với giãn cách ~1.1s/request để đủ 25/25 vào Delta/Feast.
4. **`lab28 ready` chạy trên host báo `not_ready`, chạy trong container báo
   `degraded`.** Container `api` có `LAB28_VLLM_REQUIRE_REAL=false` mặc định
   (`compose.yaml:226`), host shell thì không → mặc định `true`
   (`settings.py:200`) → vLLM probe thành `mandatory=True` → `not_ready`. Xác
   nhận qua `curl http://localhost:8000/ready` (endpoint thật) trả đúng
   `degraded`. Set `LAB28_VLLM_REQUIRE_REAL=false` khi chạy `lab28 ready` từ host
   để khớp hành vi container. **`readiness_status` ở Phase 1 không có lỗi** —
   đây thuần túy là khác biệt biến môi trường.

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

- [x] 7/7 UI endpoint trả `200`. Prometheus targets: 9/10 `up` (chỉ
      `lab28-vllm-optional` `down`, đúng dự kiến).
- [ ] Commit mốc 2 — **bỏ qua**: Phase 3 không tạo thay đổi trong repo (state
      runtime nằm trong Docker volume, đúng thiết kế `.gitignore`). `git status`
      vẫn sạch sau Phase 3.

---

## Phase 4 — Full data/ML: J1–J5 — ❌ ĐÃ THỬ, KHÔNG KHẢ THI TRÊN MÁY NÀY

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

### Kết quả thực tế — 2 lần thử, cả 2 lần OOM

**RAM host chỉ 7.7 GiB, và Docker Desktop giới hạn WSL2 VM ở ~3.68 GiB** (khoảng
50% RAM host theo mặc định) — không phải 7.7 GiB đầy đủ như kỳ vọng ban đầu. Core
stack một mình đã dùng ~2.4–2.5 GiB trong giới hạn đó, chỉ còn ~1.2 GiB đệm.

**Lần 1 — `--profile full` (spark-connect + airflow):**
- Build phải tách riêng từng service (`docker compose build airflow`) vì lặp lại
  đúng lỗi containerd race đã gặp ở Phase 3.
- 11/13 container lên `healthy`; `airflow` kẹt "starting" hơn 26 phút.
- Docker daemon bắt đầu trả `502 Bad Gateway`, rồi **`spark-connect` bị kill
  (exit 137 = SIGKILL/OOM)**, sau đó **`kafka` cũng bị kill** — OOM lan sang cả
  core stack đang chạy tốt.
- Xử lý: `docker compose stop airflow spark-connect`, `docker compose start kafka`
  → core stack khôi phục hoàn toàn (`/ready` lại `degraded` như cũ).

**Lần 2 — chỉ `airflow` một mình (không kèm `spark-connect`), theo yêu cầu thử lại
có kiểm soát hơn:**
- Docker daemon treo **~5 phút** không phản hồi cả `docker ps` trần trụi (không
  qua compose) — nặng hơn lần 1.
- `airflow` chạy 46 phút vẫn `unhealthy`, không bao giờ qua nổi health check.
- **`kafka` lại bị OOM-kill (exit 137) lần thứ hai.**
- Người dùng yêu cầu dừng airflow hẳn (`không chạy airflow nữa`) — đã dừng
  container, khởi động lại `kafka`, xác nhận core stack khôi phục.

**Kết luận: không thử chạy Airflow trên máy này nữa dưới bất kỳ hình thức nào**
(kể cả một mình, không kèm Spark). Phase 4 (J1–J5, và evidence IP01–04) bắt buộc
chuyển sang máy chung/hạ tầng giảng viên có RAM đủ 12–16 GiB như README khuyến
nghị.

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

## Phase 6 — Evidence, load test, demo, nộp bài — 🟡 Một phần (chờ Phase 4/5)

```text
uv run lab28 evidence
uv run lab28 integration
uv run python load-tests/run_profile.py --requests 200 --workers 8
uv run python load-tests/run_profile.py --requests 200 --workers 16
```

Ghi kèm số đo: P50/P95/P99, CPU/RAM của API, Kafka lag, error rate, **cấu hình
phần cứng, model, dataset, concurrency, warm-up** (theo `runbooks/performance.md`).
Không suy ra capacity production từ laptop.

### Kết quả thực tế — 4/10 evidence, `lab28 integration` score 67

- [x] `evidence/ip05-qdrant-search.json`, `ip06-mlflow-release.json`,
      `ip07-vllm-identity.json` (nội dung `unreachable` — trung thực, chưa nối
      vLLM), `integration-report.json` — 4 file `lab28 evidence` tự sinh được từ
      chính process CLI, không cần Airflow.
- [x] `lab28 integration`: **4/6 verified `ready`** (IP01, IP04, IP05, IP06),
      IP03/IP07 `not_ready` (đúng dự kiến), IP02/IP08/IP09/IP10 `unverified`,
      score `67`.
- [x] Load test `run_profile.py` (nhắm `/ready` qua gateway): 8 workers →
      67/200 `200`; 16 workers → 19/200 `200`, phần còn lại bị `429` (Envoy
      rate-limit 10 req/s là bottleneck chi phối, không phải backend — tăng
      worker chỉ làm tỷ lệ từ chối cao hơn). P50 ~9–10ms, P95/P99 800–1300ms.
      **Lưu ý khi viết báo cáo:** script gộp mọi lỗi (kể cả `429`) vào bucket
      `"0"` do `except Exception: status = 0` chung — không tách được số `429`
      thật từ status_counts, cần nêu rõ giới hạn này trong phần bottleneck
      analysis thay vì trích số liệu như thể đó là lỗi kết nối.
- [ ] 6 evidence còn lại (`ip01`, `ip02`, `ip03`, `ip04-feast-online`, `ip08`,
      `ip09` × 2, `ip10`) — không lấy được, lý do **không phải lỗi code**:

  **`integration-tests/conftest.py` có fixture `stack_is_up` (`autouse=True`,
  `scope="session"`)** chặn *toàn bộ* suite nếu Airflow không reachable — kể cả
  test không đụng tới Airflow (`test_gateway_rate_limit.py` cho IP08,
  `test_prometheus_targets.py` cho IP09). Đây là thiết kế có chủ đích ("never
  skips itself into a false green"), không phải bug.

  | Nhóm nguyên nhân | IP | Cách gỡ |
  |---|---|---|
  | Cần Airflow **chạy pipeline thật** (không chỉ sống) | IP01, IP02, IP03, IP04 | Phase 4 trên máy đủ RAM |
  | Chặn lây bởi gate `stack_is_up` dù bản thân không cần pipeline | IP08, IP09 | Gỡ được ngay khi Airflow chỉ cần *healthy* (nhẹ hơn IP01-04) |
  | Cần Airflow **+** vLLM thật cùng lúc (span coverage đủ 11 span) | IP10 (test mang marker `gpu`, không phải `langsmith`) | Phase 4 **+** Phase 5 |
  | Cần vLLM thật, độc lập hoàn toàn với Airflow | IP07 | Phase 5 — làm được ngay, không chờ Phase 4 |
  | Cần credential ngoài do giảng viên cấp | Nhánh LangSmith của IP10 (`test_the_langsmith_export_leg_is_configured_and_healthy`, không ghi `ip10-trace.json`) | Không phải lỗi của sinh viên — báo `UNVERIFIED` là **đúng theo rubric** (`gate_note` trong `integration-matrix.yaml` + `SUBMISSION.md` nói rõ điều này) |

### `evidence/` có nên bỏ khỏi `.gitignore` không? — **Có, đã sửa**

Kết luận trước đó (giữ nguyên, nộp evidence qua kênh riêng) chỉ đúng nếu có nơi
đính kèm tách biệt khỏi repo. **Thực tế: kênh nộp bài chỉ nhận link repo, không
có chỗ đính kèm riêng** — nên nếu `evidence/` vẫn bị `.gitignore`, 10 file bằng
chứng bắt buộc trong `SUBMISSION.md` (mục 2) sẽ **không bao giờ tới tay người
chấm**. Đây là ràng buộc thực tế quan trọng hơn suy luận lý thuyết từ tài liệu.

Đã sửa `.gitignore`: bỏ dòng `evidence/`, giữ nguyên `.venv/`, `.lab28/`,
`__pycache__/`, `*.pyc`, `.pytest_cache/`, `.ruff_cache/`, `readiness-report.json`
— các mục này vẫn phải ở ngoài Git vì là DB/cache/venv thật sự (Quy tắc #3 trong
README: không đưa dữ liệu tạm, cơ sở dữ liệu, bộ nhớ đệm lên Git), khác bản chất
với evidence — evidence là **deliverable bắt buộc** theo `SUBMISSION.md`, không
phải state tạm.

**Quy trình commit evidence — làm ở cuối, không làm giữa chừng:**

1. Không commit ngay bây giờ — hiện chỉ có 4/10 file, và `ip07-vllm-identity.json`
   đang phản ánh trạng thái *chưa nối vLLM* (đúng nhưng chưa hoàn chỉnh).
2. Sau khi Phase 4 (Airflow trên máy chung) và Phase 5 (vLLM GPU) xong, chạy lại
   **toàn bộ** `lab28 evidence` một lần cuối để có bản mới nhất, khớp với đúng
   run/trace/model ID sẽ trình bày khi demo.
3. Commit đúng một lần ở bước đó — tránh nhiều commit evidence rải rác qua các
   lần chạy thử nghiệm, giữ lịch sử Git sạch và bản cuối luôn là bản live thật.
4. Vẫn tuyệt đối không sửa tay nội dung JSON trong `evidence/` — mọi thay đổi
   phải đến từ chạy lại `lab28 evidence`/`pytest integration-tests`.

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
| `docker compose up --build` lỗi containerd race | `image "...:latest": already exists` khi export | Build từng service riêng (`docker compose build <service>`) trước khi `up -d --wait` |
| `lab28 release` crash `UnicodeEncodeError` | MLflow in emoji, console Windows dùng `cp1252` | `PYTHONIOENCODING=utf-8` trước mọi lệnh `uv run lab28 ...` |
| `seed --via-gateway` báo `rejected` (`429`) | Envoy rate-limit `10 tokens/s` dùng chung toàn route (`gateway/envoy.yaml:45-48`), 25 request gửi gần như đồng thời | Đây là bằng chứng IP08 cần có, không phải bug; gửi bù phần thiếu có giãn cách ~1.1s/request |
| `lab28 ready` (host) báo `not_ready`, container báo `degraded` | Container `api` có `LAB28_VLLM_REQUIRE_REAL=false` mặc định, host shell thì không (`settings.py:200` default `true`) | `LAB28_VLLM_REQUIRE_REAL=false` khi chạy CLI từ host, hoặc tin `/ready` thật qua HTTP thay vì CLI |
| `integration-tests` báo lỗi dù test không cần Airflow | `stack_is_up` fixture (`conftest.py`, `autouse`, `session`) chặn toàn bộ suite nếu Airflow không reachable | Không có cách né hợp lệ — chờ Phase 4 |
| Airflow OOM-kill `kafka` dù chạy một mình | WSL2 VM giới hạn ~3.68 GiB (không phải RAM host đầy đủ), Airflow cần nhiều RAM hơn mức đệm còn lại | Không thử lại trên máy 7.7 GiB — chuyển máy chung |

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
