# ANSWERS — Day 28 Track 2

- **Học viên:** Đoàn Quốc Việt (2A202601623) — làm **cá nhân**, kiêm toàn bộ 5 vai.
- **Nhánh:** `ca-nhan-viet`
- **Máy chạy:** Windows 11, 16 vCPU, 15.3 GiB RAM, Docker Desktop 29.4.0 (WSL2, 7.4 GiB cấp cho VM), Python 3.11.8 qua `uv` 0.12.9.
- **Thời điểm thu bằng chứng:** 2026-09-03 → 2026-09-04 (giờ VN). Mọi số trong tài liệu này lấy từ `evidence/`, không có số nào viết tay.

---

## 1. Kết quả kiểm thử

| Bộ | Lệnh | Kết quả |
|---|---|---|
| Starter | `uv run pytest starter-tests -q` | **4 passed** |
| Fast | `uv run pytest starter-tests tests -q` | **87 passed** |
| Live | `uv run pytest integration-tests -m "not gpu and not langsmith" -q` | **56 passed, 16 deselected** (561s) |
| Lint | `uv run ruff check .` | All checks passed |
| Matrix | `uv run python scripts/verify_matrix.py` | 245 checks passed |
| Portability | `uv run python scripts/check_portability.py` | OK |
| Manifests | `uv run python scripts/validate_manifests.py` | passed |

16 test bị `deselected` là gate GPU (IP07) và gate LangSmith (IP10) — xem mục 5.

---

## 2. Trạng thái 10 integration point

| IP | Trạng thái | Bằng chứng | Giá trị thật |
|---|---|---|---|
| IP01 HTTP → Kafka | ✅ | `ip01-kafka-consume.json` | `data.raw` p0 offset 40, header `idempotency-key` + `traceparent` + `schema_version` |
| IP02 Kafka → Airflow | ✅ | `ip02-airflow-run.json` | DAG run `it-3c334213`, state `success`, 4/4 task success |
| IP03 Airflow/Spark → Delta | ✅ | `ip03-delta-history.json` | `feedback` v15 (16 commit), `documents` v9; có time travel diff |
| IP04 Delta → Feast | ✅ | `ip04-feast-online.json` | entity `it-j1-6fa1c01a`, status `PRESENT`, `delta_version: 10` |
| IP05 Delta → Qdrant | ✅ | `ip05-qdrant-search.json` | 20 point, ID UUID tất định từ `doc_id` |
| IP06 Eval → MLflow | ✅ | `ip06-mlflow-release.json` | `lab28-rag-release` v3 = `champion` (sau rollback từ v4) |
| IP07 → vLLM thật | ⚠️ **UNVERIFIED** | `ip07-vllm-identity.json` | Không có GPU endpoint — xem mục 5 |
| IP08 Client → Envoy | ✅ | `ip08-gateway.json` | 30 request → 10 accepted / 20 × HTTP 429, có `x-request-id` |
| IP09 → Prometheus/Grafana | ✅ | `ip09-prometheus-targets.json`, `ip09-grafana-dashboards.json` | mọi job `health: up` |
| IP10 → OTLP trace | ✅ (local) / ⚠️ LangSmith UNVERIFIED | `ip10-trace.json` | trace `af62c626d9214819a2266fe886c19506`, 11 span, 3 service |

**8/10 xác minh đầy đủ trên hạ tầng thật. 2 điểm là gate môi trường**, báo `UNVERIFIED` theo đúng `SUBMISSION.md` thay vì giả lập.

---

## 3. Bốn hàm đã hoàn thiện và lý do thiết kế

Toàn bộ nằm trong `src/lab28_platform/integration_tasks.py`.

### `event_headers` (IP01 + IP10)

Luôn trả `idempotency-key`; **bỏ hẳn** `traceparent` khi không có trace thay vì gửi chuỗi rỗng.

**Trade-off:** một header rỗng vẫn "đúng cú pháp Kafka" nhưng sai chuẩn W3C — consumer sẽ parse ra context rác và nối span vào một trace không tồn tại. Thà đứt trace một cách rõ ràng còn hơn có một trace sai mà không ai phát hiện. Hàm trả `list` (không phải tuple) vì `event_bus.py:180` gọi `.append()` để thêm `schema_version` ngay sau đó.

### `dedupe_latest` (IP03)

Một vòng lặp duy nhất, giữ event có `(occurred_at, event_id)` lớn nhất cho mỗi `idempotency_key`, kết quả sắp theo key.

**Trade-off:** so sánh tuple thay vì chỉ `occurred_at` để hai event trùng timestamp vẫn cho cùng kết quả bất kể Kafka giao theo thứ tự nào — Kafka chỉ đảm bảo thứ tự *trong một partition*, mà `data.raw` có 3 partition. Sắp theo key làm MERGE source tất định, tái lập được giữa các lần chạy. Đây là điều kiện sống còn: Delta MERGE **ném lỗi** nếu source có hai dòng khớp cùng một dòng target.

### `feast_online_request` (IP04)

Dùng lại `FEATURE_REFS` từ `contracts.py` thay vì viết lại danh sách feature.

**Trade-off:** import thêm một hằng số để đổi lại chỉ có một nguồn sự thật. Nếu registry đổi tên feature mà request viết cứng, lỗi sẽ hiện ra ở runtime dưới dạng `NOT_FOUND` khó truy — còn cách này thì sai lệch bị bắt ngay khi import.

### `readiness_status` (IP07 + IP08)

Thoát sớm ở `not_ready`; `degraded` chỉ khi mọi lỗi đều không bắt buộc.

**Trade-off:** thoát sớm nghĩa là không đếm hết số lỗi — nhưng `/ready` không cần đếm, nó cần một phán quyết. Chi tiết từng component đã nằm trong `ReadinessResponse.components`. Ba trạng thái tách bạch mới cho phép gateway rút pod khỏi rotation khi `not_ready` mà vẫn giữ traffic khi `degraded`.

---

## 4. Sự cố, khôi phục và chứng minh không mất dữ liệu

Kịch bản theo `runbooks/failure-injection.md`, mốc thời gian thật:

| Thời điểm | Hành động | Quan sát |
|---|---|---|
| T0 | baseline | `/ready` = `degraded` (chỉ vLLM lỗi) |
| 00:09:56 | `docker compose stop feast` | `/ready` = `degraded`, component `feast` = `unreachable: ConnectError` |
| 00:10:18 | `up -d --wait feast` | container `Healthy` |
| T+~20s | kiểm tra lại | `/ready` = `degraded`, **chỉ còn vLLM** — Feast đã phục hồi |

**Dự đoán trước khi inject:** Feast là dependency *không bắt buộc*, nên hệ thống phải xuống `degraded` chứ không phải `not_ready` — đúng như `readiness_status` quy định. Kết quả khớp dự đoán.

**Chứng minh không mất dữ liệu** (sau khi khôi phục):

```text
feedback : latest_version 15, 16 commits
documents: latest_version  9, 10 commits
qdrant   : 20 points
```

Không có version nào bị lùi, không commit nào biến mất. Lý do cấu trúc: `process_batch_then_commit` chỉ commit offset Kafka **một lần, ở cuối, và chỉ khi không có exception nào thoát ra**. Giết tiến trình ở bất kỳ điểm nào trước commit thì đúng lô event đó được giao lại — và `dedupe_latest` + Delta MERGE làm việc giao lại đó vô hại. Đây chính là điều J2 kiểm chứng (9 passed): replay không làm tăng số dòng.

Bằng chứng: `evidence/failure-during.json`, `evidence/failure-after.json`.

---

## 5. Hai gate môi trường — vì sao báo UNVERIFIED

### IP07 — vLLM (gate GPU)

Máy không có NVIDIA GPU và lớp chưa cấp endpoint vLLM dùng chung. `LAB28_VLLM_REQUIRE_REAL` mặc định `true`, nên `probe_vllm` từ chối bất kỳ endpoint nào không tự chứng minh được qua `/version` và metric tiền tố `vllm:`.

Hệ quả: `uv run lab28 ready` trả **`not_ready`** — và đó là kết luận **đúng**, không phải lỗi cấu hình. Tôi **không** hạ `LAB28_VLLM_REQUIRE_REAL=false` để "làm đẹp" trạng thái, cũng không dựng server giả OpenAI-compatible: theo rubric, làm giả vLLM là 0 điểm phần đó. 3 test trong J1 và 3 test khác skip với lý do ghi rõ: `gpu gate: endpoint is not a verifiable vLLM build`.

Cách gỡ: chạy `vllm serve Qwen/Qwen3-4B-Instruct-2507` trên Kaggle T4 theo `KAGGLE_GPU_EXTENSION.md`, trỏ `LAB28_VLLM_BASE_URL` vào đó.

### IP10 — LangSmith

Chân OTLP nội bộ **đã chứng minh đầy đủ**: `ip10-trace.json` có một trace ID duy nhất mang 11 span xuyên 3 service (`lab28-gateway`, `lab28-api`, `lab28-airflow`). Chỉ chân xuất sang LangSmith là `UNVERIFIED` vì không có `LANGSMITH_API_KEY`.

---

## 6. Hiệu năng và nút thắt

200 request, endpoint `/ready`. Số liệu đầy đủ ở `evidence/load-profile.json`.

| Đích | Workers | 200 OK | P50 | P95 | P99 |
|---|---:|---:|---:|---:|---:|
| Gateway :8080 | 8 | 58/200 | 12.8 ms | 1640 ms | 4798 ms |
| Gateway :8080 | 16 | 57/200 | 22.2 ms | 1674 ms | 1833 ms |
| API :8000 | 8 | **200/200** | 652 ms | 872 ms | 5338 ms |
| API :8000 | 16 | 198/200 | 1354 ms | 1807 ms | 1999 ms |

### Phân tích

**Nút thắt 1 — rate limit gateway là ràng buộc chi phối, không phải năng lực API.** Qua gateway chỉ ~57/200 request qua được; 143 request còn lại nhận HTTP 429 từ token bucket 10 token/giây trong `gateway/envoy.yaml`. P50 12.8 ms là *độ trễ của việc bị từ chối*, không phải độ trễ phục vụ — một chỉ số rất dễ đọc nhầm thành "hệ thống nhanh". Đây cũng chính là lý do `lab28 seed --via-gateway` luôn có 4 bản ghi bị 429: nó bắn 25 request liên tiếp vào một bucket 10/s.

**Nút thắt 2 — `/ready` đắt vì probe MLflow tải artifact mỗi lần gọi.** Đo trực tiếp vào API: P50 652 ms ở 8 worker, tăng gấp đôi lên 1354 ms ở 16 worker trong khi P95 gần như không đổi. Độ trễ tăng tuyến tính theo concurrency là dấu hiệu kinh điển của **hàng đợi**, không phải nghẽn CPU — API đã bão hòa ở khoảng 8 request đồng thời. Nguyên nhân: `/ready` gọi 5 probe *tuần tự*, trong đó `probe_mlflow` thực hiện `Downloading artifacts` qua HTTP mỗi lần.

**Đề xuất khắc phục (chưa triển khai, ngoài phạm vi bài):** cache kết quả probe MLflow trong 5–10 giây và chạy 5 probe song song bằng `asyncio.gather`.

**Cảnh báo diễn giải:** không được suy ra capacity production từ các số này. P99 5338 ms ở 8 worker phản ánh cold start của laptop và tranh chấp tài nguyên với 14 container trên cùng máy, không phải đặc tính của hệ thống.

---

## 7. Kubernetes / GitOps

`uv run python scripts/validate_manifests.py` → passed. Manifest ở `deploy/kubernetes/base/`:

- **Bảo mật:** `runAsNonRoot`, `seccompProfile: RuntimeDefault`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`, `NetworkPolicy` giới hạn traffic.
- **Ba probe tách bạch:** `/health` (liveness, không chạm dependency), `/ready` (readiness, kiểm tra dependency), `/health` với `failureThreshold: 24` (startup, cho phép 120 s khởi động).
- **Khả dụng:** `replicas: 2`, `HorizontalPodAutoscaler`, `PodDisruptionBudget`.
- **GitOps:** `gitops/application.yaml` — Argo CD với `selfHeal: true`, `prune: true`, `targetRevision: refs/tags/v3.0.0` (tag bất biến, không phải `HEAD`), `revisionHistoryLimit: 5`.

**Rollback đã chứng minh thật ở tầng model** (không sửa một dòng code nào):

```text
uv run lab28 release   → lab28-rag-release v4, alias champion
uv run lab28 rollback  → lab28-rag-release v3, alias champion
```

Alias `champion` là điểm gián tiếp: serving path đọc alias chứ không đọc số version, nên đổi alias là đổi model đang phục vụ. Ở tầng cluster, cơ chế tương đương là revert `targetRevision` trong Git rồi để Argo CD sync — `revisionHistoryLimit: 5` giữ 5 bản để quay lại.

**Chưa xác minh được:** drift/self-heal thật cần một cluster Kubernetes đang chạy và Argo CD. Máy này chỉ có Docker Compose, nên phần K8s dừng ở mức **static validation** — khai báo trung thực chứ không suy diễn.

---

## 8. Sự cố môi trường đã gặp và cách chẩn đoán

Ghi lại vì đây là phần "đọc tín hiệu để tìm đúng owner" mà bài học nhắm tới.

### 8.1 Airflow drain được 0 message dù topic có 88 message

**Triệu chứng:** J1 fail, thiếu span `lab28.kafka.consume` và `lab28.spark.delta_merge`. Nhưng log task lại báo `success` với `{'polled': 0}`.

**Chẩn đoán:** `kafka-consumer-groups.sh --describe --group lab28-pipeline` → `GroupIdNotFoundException`, trong khi `kafka-get-offsets.sh` cho thấy `data.raw` có 88 message (27+37+24). Nghĩa là consumer **chưa từng join** group, chứ không phải đã đọc hết.

**Nguyên nhân gốc:** `BatchConsumer.poll_batch` mặc định `idle_polls=3, poll_timeout=1.0` → bỏ cuộc sau 3 giây. Lần join đầu tiên của một consumer group mới trên Kafka 4.x (tìm coordinator + JoinGroup + SyncGroup) lâu hơn 3 giây, nên vòng lặp kết thúc trước khi được assign partition.

**Kiểm chứng:** chạy `BatchConsumer` trực tiếp trong container Airflow → `polled=50 in 0.2s` khi group đã tồn tại. Chạy lại J1 → **12 passed**.

**Bài học:** đây là lỗi *lần chạy đầu tiên*, không tái hiện sau đó. Trong production nó sẽ biểu hiện thành một batch rỗng im lặng mỗi khi group bị xóa hoặc đổi tên — đúng loại lỗi mà "task SUCCESS" che giấu, và là lý do rubric nhấn mạnh `SUCCESS` chưa đủ.

### 8.2 Grafana healthy nhưng host không truy cập được

**Triệu chứng:** `test_grafana_is_provisioned_from_configuration` fail với `httpx.ReadTimeout`; log Grafana sạch, CPU 1.35%, RAM 248 MiB.

**Chẩn đoán:** `wget` **trong container** → `{"database": "ok"}`. `curl` **từ host** → timeout sau 25 s. Vậy vấn đề ở lớp port-forward chứ không ở Grafana.

**Nguyên nhân gốc:** `Get-NetTCPConnection -LocalPort 3000` cho thấy **một tiến trình `node` đang nghe trên `::1:3000`**, còn Docker nghe trên `::`. Trên Windows, `localhost` phân giải `::1` trước → request đi vào server node.

**Khắc phục:** theo đúng hướng dẫn README — tạo `ports.local.env` (đã thêm vào `.gitignore`), đổi **chỉ cổng host** sang `LAB28_GRAFANA_PORT=3001`, chạy lại với `--env-file ports.local.env`. Suite đầy đủ → **56 passed**.

---

## 9. Khoảng cách so với production

1. **Không có auth.** Gateway chưa có mTLS/JWT; `/api/v1/*` mở hoàn toàn. Rate limit theo listener chứ không theo tenant, nên một client có thể làm đói toàn bộ phần còn lại.
2. **Rate limit là local, không phân tán.** `local_rate_limit` của Envoy tính theo từng instance. Chạy N replica thì hạn mức thực tế thành N × 10 rps. Production cần một rate limit service dùng chung.
3. **Kafka một broker, replication factor 1.** Mất broker là mất dữ liệu. Production tối thiểu 3 broker, RF 3, `min.insync.replicas=2`.
4. **Không có backpressure ở ingestion.** API trả 202 rồi mới produce; nếu hàng đợi producer đầy, `BufferError` thành 503 nhưng client đã coi như được nhận.
5. **`/ready` tự nó là một vector DoS.** Mỗi lần gọi kéo theo một artifact download từ MLflow. Kubernetes probe mỗi 10 s × N pod sẽ tự tạo tải lên MLflow.
6. **Không có retention/GDPR cho Delta.** `feedback` chứa text người dùng, chưa có chính sách xóa hay `VACUUM`.
7. **DLQ chưa có alert.** `data.raw.dlq` tồn tại và replay được, nhưng không có alert khi nó tăng — dead letter im lặng là dead letter bị bỏ quên.
8. **Embedding model ghim theo commit hash nhưng chưa có kế hoạch migrate.** Đổi model là phải reindex toàn bộ; chưa có quy trình chạy song song hai collection.
9. **Không đo được chất lượng.** 13 document không đủ để đánh giá retrieval; mọi con số accuracy từ 12 bản feedback đều là nhiễu (chính `data/README.md` cũng nói vậy).

---

## 10. Phân công

Làm **cá nhân**, một người thực hiện tuần tự đủ 5 vai theo `docs/team-role-cards.md`:

| Vai | Phạm vi | Sản phẩm |
|---|---|---|
| Ingestion & Orchestration | IP01–IP02 | `event_headers`; chẩn đoán 8.1 |
| Data & ML | IP03, IP04, IP06 | `dedupe_latest`, `feast_online_request`; promotion/rollback v4→v3 |
| Serving & Retrieval | IP05, IP07 | index 13 document; xác định IP07 là gate GPU và giữ nguyên `not_ready` |
| Platform & Observability | IP08–IP10 | chẩn đoán 8.2; load profile; validate manifest |
| Presenter / Incident Commander | — | tài liệu này; kịch bản sự cố mục 4 |

---

## 11. Tái lập kết quả

```text
uv sync --frozen --python 3.11 --extra dev --extra integration --no-editable
uv run pytest starter-tests tests -q
uv run ruff check . && uv run python scripts/verify_matrix.py
uv run python scripts/check_portability.py && uv run python scripts/validate_manifests.py

docker compose --env-file ports.template --profile full up -d --build --wait
uv run lab28 topics && uv run lab28 index --source file && uv run lab28 release && uv run lab28 seed
uv run pytest integration-tests -m "not gpu and not langsmith" -q
uv run lab28 evidence && uv run lab28 integration
```

Nếu cổng 3000 bị chiếm, copy `ports.template` thành `ports.local.env`, đổi `LAB28_GRAFANA_PORT`, và thêm `LAB28_GRAFANA_URL=http://localhost:<cổng mới>` khi chạy test.

**Lưu ý về lần chạy đầu:** nếu J1 fail vì thiếu span `lab28.kafka.consume`, đó là mục 8.1 — chạy lại J1, consumer group đã tồn tại sẽ join ngay.

Không có secret, `.env`, database, cache hay weight model nào được commit. `.gitignore` đã loại `.lab28/`, `.venv/`, `evidence/`, `ports.local.env`.
