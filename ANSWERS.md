# Báo Cáo Nộp Bài (Submission Report) — Day 28 Track 2
**Platform Integration & Production Readiness**

---

## 1. Thông Tin Nhánh & Phân Công Vai Trò

- **Họ và tên học viên:** Võ Quốc Huy (2A202601188)
- **Hình thức thực hiện:** Cá nhân
- **Repository Private:** `https://github.com/Huy0123/Track2_Day28_2A202601188_VoQuocHuy`
- **Nhánh làm việc (Branch):** `ca-nhan-huy`
- **URL nhánh nộp bài:** `https://github.com/Huy0123/Track2_Day28_2A202601188_VoQuocHuy/tree/ca-nhan-huy`

### Các vai trò đã đảm nhiệm (Role Coverage):
1. **Ingestion & Orchestration (IP01, IP02):**
   - Triển khai gắn `idempotency-key` và `traceparent` (W3C context) vào Kafka headers (`data.raw`).
   - Cấu hình và giám sát Airflow 3 DAG pipeline `lab28_ingestion_pipeline` đọc từ Kafka và cập nhật asset events.
2. **Data & ML (IP03, IP04, IP06):**
   - Xây dựng thuật toán loại trừ bản ghi trùng lặp `dedupe_latest` cho Spark Delta MERGE.
   - Định dạng truy vấn Feast online feature service `asker_activity_v1`.
   - Đăng ký và promote release mô hình `lab28-rag-release` phiên bản champion trên MLflow Model Registry.
3. **Serving & Retrieval (IP05, IP07):**
   - Đánh chỉ mục và truy vấn lai (hybrid dense + sparse BM25) trên Qdrant với deterministic point ID.
   - Xử lý endpoint vLLM theo cơ chế graceful degradation khi chạy local CPU không có GPU.
4. **Platform & Observability (IP08, IP09, IP10):**
   - Cấu hình định tuyến, request correlation ID (`x-request-id`) và rate-limiting trên Envoy Gateway.
   - Giám sát metrics Prometheus, Grafana dashboards và OpenTelemetry W3C distributed tracing qua Jaeger.
5. **Presenter / Incident Commander:**
   - Thu thập đầy đủ bằng chứng (evidence bundle), phân tích chỉ số tải (P50/P95/P99) và kịch bản phục hồi sự cố.

---

## 2. Kết Quả Kiểm Thử Phần Mã & Integration Matrix

Toàn bộ các bộ kiểm thử và kiểm tra tĩnh đã được thực thi và vượt qua 100%:

| Bộ kiểm thử / Công cụ kiểm tra | Lệnh thực thi | Kết quả | Chi tiết |
|---|---|:---:|---|
| **Linter** | `uv run ruff check .` | **PASSED** | Code formatting và typing chuẩn chỉ |
| **Matrix Verification** | `uv run python scripts/verify_matrix.py` | **PASSED** | 245/245 checks passed, khớp 100% contract matrix |
| **Portability Check** | `uv run python scripts/check_portability.py` | **PASSED** | Độc lập nền tảng (Windows/macOS/Linux) |
| **Kubernetes & GitOps** | `uv run python scripts/validate_manifests.py` | **PASSED** | K8s Gateway & ArgoCD manifests hợp lệ |
| **Starter & Unit Tests** | `uv run pytest starter-tests tests -q` | **PASSED** | **87/87 passed** (gồm 22 test Delta idempotency) |
| **Full Integration Suite** | `uv run pytest integration-tests -m "not gpu and not langsmith" -q` | **PASSED** | **56/56 passed** (16 deselected theo rubric GPU/LangSmith) |

---

## 3. Danh Mục 10 Bằng Chứng Tích Hợp (Evidence Bundle)

Được tạo tự động và lưu trữ tại thư mục `evidence/`:

1. **`evidence/ip01-kafka-consume.json` (IP01 — HTTP Ingestion → Kafka):**
   - Topic: `data.raw`, Partition: 1, Offset: 9
   - Headers: `traceparent: "00-60bd4fa7c4714320b8be6480b82b0367-ae53dfa2f9a4f4fe-01"`, `idempotency-key: "it-j1-02ed89e9"`
2. **`evidence/ip02-airflow-run.json` (IP02 — Kafka → Airflow Pipeline):**
   - DAG Run ID: `it-dd50bc0a`, State: `success`
   - Tasks hoàn thành: `drain_kafka_into_delta`, `refresh_online_features`, `index_new_documents`, `announce_processed_batch`.
3. **`evidence/ip03-delta-history.json` (IP03 — Pipeline → Delta Lake):**
   - Bảng feedback đạt Version 7, bảng documents đạt Version 4; ghi nhận đầy đủ lịch sử phép toán `MERGE`.
4. **`evidence/ip04-feast-online.json` (IP04 — Lakehouse → Feast Feature Store):**
   - Query online features `asker_activity_v1` cho entity `asker_id` trả về đúng các thuộc tính `feedback_count`, `avg_rating`, `delta_version`.
5. **`evidence/ip05-qdrant-search.json` (IP05 — Data → Qdrant Vector Store):**
   - 16 documents points, truy vấn hybrid tìm kiếm ngữ nghĩa với score tương đồng chính xác.
6. **`evidence/ip06-mlflow-release.json` (IP06 — Evaluation → MLflow Model Registry):**
   - Run ID: `c56e75014b03416cba2892bfe1f90020`, model `lab28-rag-release` version `1` được gán alias `champion`.
7. **`evidence/ip07-vllm-identity.json` (IP07 — Serving → vLLM):**
   - Trạng thái: Báo `UNVERIFIED` do môi trường local CPU không có GPU rời, đáp ứng đúng yêu cầu rubric không được dùng mock server.
8. **`evidence/ip08-gateway.json` (IP08 — Client → Envoy Gateway):**
   - Chặn rate-limit HTTP 429 khi vượt ngưỡng tokens và trả về mã tương quan `x-request-id`.
9. **`evidence/ip09-prometheus-targets.json` & `ip09-grafana-dashboards.json` (IP09 — Metrics & Alerts):**
   - Toàn bộ targets (API, Kafka exporter, Feast, Pushgateway) đều UP; dashboards và alerts cấu hình sẵn.
10. **`evidence/ip10-trace.json` (IP10 — OpenTelemetry Distributed Tracing):**
    - Trace ID: `60bd4fa7c4714320b8be6480b82b0367` đi xuyên suốt từ Gateway → API → Kafka → Airflow → Spark Delta.
11. **`evidence/integration-report.json`:** Báo cáo tổng thể toàn bộ 10 Integration Points.

---

## 4. Chứng Minh Luồng Đúng (Happy Path) & Chống Trùng Lặp (Replay-Safe)

- **Trace ID xuyên suốt:** `60bd4fa7c4714320b8be6480b82b0367`
- **DAG Run ID:** `it-dd50bc0a`
- **Delta Lake Table Version:** `v7`
- **MLflow Version:** `v1` (Run ID: `c56e75014b03416cba2892bfe1f90020`)
- **Chứng minh Replay-Safe (Idempotency):**
  - Khi Kafka phát lại (replay) cùng một payload hoặc một chuỗi sự kiện trùng lặp, hàm `dedupe_latest` gom nhóm theo `idempotency_key`, so sánh cặp `(occurred_at, event_id)` để chỉ giữ lại sự kiện mới nhất.
  - Phép `MERGE INTO` của Spark Delta áp dụng mệnh đề `WHEN MATCHED THEN UPDATE` thay vì append, đảm bảo số dòng trong bảng Delta không bị tăng lên khi replay nhiều lần (kiểm chứng qua suite test `test_j2_idempotent_replay.py`).
- **Chỉ số kiểm thử tải (Load Test Profile):**
  - Tổng số request: **200 requests** với **8 workers** song song.
  - Tỷ lệ thành công: **99%** (198/200 requests HTTP 200).
  - Độ trễ:
    - **P50:** 876.5 ms
    - **P95:** 1932.6 ms
    - **P99:** 2925.2 ms

---

## 5. Kịch Bản Sự Cố, Dấu Hiệu Quan Sát & Khôi Phục (Incident & Recovery)

- **Sự cố mô phỏng:** Dịch vụ vLLM inference endpoint không khả dụng (do môi trường local không có card GPU chuyên dụng).
- **Dấu hiệu quan sát (Signals):**
  - Gọi endpoint `/ready` qua Envoy Gateway (`http://localhost:8080/ready`) trả về mã HTTP 200 với payload mang cờ `"status": "degraded"`.
  - Thành phần `vllm` trong danh sách components báo `ready: false` kèm chi tiết `unreachable: ConnectError`.
  - Không có cảnh báo crash pod trên Envoy Gateway, gateway vẫn duy trì định tuyến các request đọc tài liệu và phản hồi bình thường.
- **Nguyên nhân gốc rễ (Root Cause):**
  - vLLM container không được khởi chạy cục bộ do thiếu driver NVIDIA/CUDA.
- **Cơ chế xử lý & Khôi phục (Graceful Degradation):**
  - Nhờ cơ chế phân loại mức độ nghiêm trọng trong hàm `readiness_status`: `vllm` được đánh dấu là probe không bắt buộc (`mandatory=False`) khi chạy ở chế độ fallback.
  - Hệ thống tự động chuyển sang chế độ suy thoái (degraded mode), các luồng ingestion, xử lý batch, tính năng Feast và tìm kiếm Qdrant vẫn tiếp tục hoạt động mà **hoàn toàn không bị mất mát dữ liệu (No Data Loss)**. Khi kết nối endpoint vLLM remote (ví dụ Kaggle hoặc GPU cluster), hệ thống tự động phục hồi về trạng thái `ready`.

---

## 6. Reflection (Đúc Rút Kinh Nghiệm)

1. **Điều khó khăn nhất:**
   - Việc đồng bộ ngữ cảnh phân tán (W3C Traceparent) xuyên qua nhiều tầng công nghệ khác nhau: từ HTTP header của Gateway/FastAPI, sang Kafka record headers (dạng bytes), rồi truyền vào Airflow DAG run configuration, xuống Spark job và gắn vào metadata của dòng dữ liệu trong Delta Lake. Việc chỉ cần sai định dạng bytes/string hoặc không truyền đúng header key sẽ làm đứt gãy trace ID của toàn hệ thống.

2. **Trade-off đã lựa chọn:**
   - **Degraded vs Hard-failing:** Lựa chọn cho phép hệ thống chuyển sang trạng thái `degraded` khi thiếu vLLM phục vụ thay vì chặn toàn bộ (`not_ready`). Quyết định này giúp các đường ống dữ liệu cốt lõi (Ingestion, Delta MERGE, Feast, Qdrant) vẫn vận hành liên tục và ổn định trên máy phát triển cá nhân mà không phụ thuộc cứng vào GPU.

3. **Điều sẽ cải tiến trong tương lai:**
   - Tích hợp thêm Dead Letter Queue (DLQ) consumer tự động kích hoạt cảnh báo qua Alertmanager khi có sự kiện vi phạm contract schema thay vì chỉ lưu trữ trên Kafka topic `data.raw.dlq`.
   - Bổ sung circuit breaker cấp Gateway (Envoy) để tự động ngắt tải ngay tại biên khi downstream database/lakehouse gặp quá tải.
