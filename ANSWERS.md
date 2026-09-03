# Báo Cáo Phân Tích Kỹ Thuật — Day 28 Track 2: Platform Integration & Production Readiness

## 1. Tổng quan Kiến trúc & 10 Điểm Kết Nối (Integration Matrix)

Hệ thống RAG Platform bao gồm 5 tầng kiến trúc chính với 10 ranh giới tích hợp (Integration Points - IP01 đến IP10):

| ID | Ranh giới (Boundary) | Tầng | Contract & Giao thức | Cơ chế đảm bảo / Health Signal |
|---|---|---|---|---|
| **IP01** | Client / Gateway $\rightarrow$ Kafka | L2 Data | HTTP Ingestion $\rightarrow$ `data.raw` topic, kèm `idempotency_key` và `traceparent` | Broker reachability, topic retention cấu hình 7 ngày, ack=all |
| **IP02** | Kafka $\rightarrow$ Airflow 3 | L2 Data | `contracts.IngestionEvent` $\rightarrow$ Airflow DAG Trigger / Asset Event | Airflow health check (`/api/v2/monitor/health`), lag monitoring |
| **IP03** | Airflow/Spark $\rightarrow$ Delta Lake | L2 Data | Batch `dedupe_latest` $\rightarrow$ Spark SQL `MERGE INTO ... ON idempotency_key` | Transaction log (`_delta_log`), ACID versioning, time travel |
| **IP04** | Delta Lake $\rightarrow$ Feast | L3 ML | Delta parquet export $\rightarrow$ Feast Online Store (`asker_serving_v1`) | Feast server `/health` 200, entity lookup `asker_activity_v1` |
| **IP05** | Delta Docs $\rightarrow$ Qdrant | L2 Data | Text embedding (dense + sparse) $\rightarrow$ Qdrant Collection `lab28_documents` | Deterministic Point UUID derived from `doc_id`, score threshold |
| **IP06** | Evaluation $\rightarrow$ MLflow | L3 ML | Model packaging, metrics logging, artifact signature | Model Registry alias `champion`, semantic tags, rollback capability |
| **IP07** | RAG Engine $\rightarrow$ vLLM | L3 Serving | OpenAI-compatible API (`/v1/chat/completions`) + identity verification | vLLM `/version` probe, metric prefix `vllm:`, latency budget |
| **IP08** | Client $\rightarrow$ Envoy Gateway | L1 Ingress | Gateway API routing, rate limiting (429 Too Many Requests) | Envoy health check, request-id propagation, timeout / retry |
| **IP09** | Services $\rightarrow$ Prometheus/Grafana | L4 Obs | `/metrics` endpoint scraping, Golden Signals metrics | Alert rules (Kafka lag, error budget burn, P99 latency) |
| **IP10** | Tracing $\rightarrow$ OpenTelemetry / Jaeger | L4 Obs | W3C `traceparent` context propagation qua HTTP headers & Kafka headers | Complete distributed trace traversal từ Ingress $\rightarrow$ DB $\rightarrow$ LLM |

---

## 2. Phân Tích Các Đánh Đổi Kỹ Thuật (Architectural Trade-offs)

### 2.1. Idempotent Ingestion: Kafka Replay + Delta MERGE vs Append-Only
- **Lựa chọn:** Bắt buộc mọi event phải có `idempotency_key`. Tầng Ingestion áp dụng `dedupe_latest` trước khi đẩy vào Spark MERGE SQL (`WHEN MATCHED THEN UPDATE SET *`, `WHEN NOT MATCHED THEN INSERT *`).
- **Ưu điểm:** Cho phép Kafka consumer replay lại toàn bộ topic khi có sự cố mà không lo double-count hay sai lệch số liệu phân tích. Đảm bảo ngữ nghĩa **Exactly-Once Semantics (EOS)** tại tầng lưu trữ dữ liệu (Lakehouse) ngay cả khi Kafka chỉ cung cấp At-Least-Once delivery.
- **Đánh đổi:** Chi phí tính toán của thao tác MERGE cao hơn nhiều so với Append-Only thông thường. Cần partition Delta table và quản lý kích thước batch hợp lý để hạn chế write amplification và contention.

### 2.2. Feature Store Tách Biệt: Feast Online Store vs Query Trực Tiếp Lakehouse
- **Lựa chọn:** Dùng Spark tính toán offline feature aggregate từ Delta, sau đó định kỳ materialize sang Feast Redis/SQLite Online Store.
- **Ưu điểm:** Giảm độ trễ đọc feature phục vụ RAG từ hàng giây (khi query Delta) xuống sub-10ms (từ Online Store). Tách biệt tải giữa phân tích dữ liệu và serving thời gian thực.
- **Đánh đổi:** Chấp nhận độ trễ dữ liệu (feature freshness delay) tương ứng với chu kỳ chạy batch của Airflow/Spark. Phải quản lý thêm pipeline đồng bộ và xử lý trường hợp feature bị stale.

### 2.3. Vector Storage: Deterministic Point UUID vs Sequential/Random IDs
- **Lựa chọn:** Sử dụng hàm băm tất định (`uuid5(ID_NAMESPACE, doc_id)`) để sinh Qdrant Point ID từ `doc_id` của tài liệu.
- **Ưu điểm:** Khi tài liệu được re-index hoặc chỉnh sửa, việc ghi lại điểm vector sẽ tự động ghi đè (upsert) lên đúng point đó, ngăn chặn hiện tượng duplicate embedding points trong index.
- **Đánh đổi:** Cần thống nhất quy ước namespace và thuật toán sinh ID xuyên suốt mọi client producer.

### 2.4. Phân Tách Ngữ Nghĩa Probe: `/health` (Liveness) vs `/ready` (Readiness) vs Degraded Mode
- **Lựa chọn:**
  - `/health`: Liveness probe cực nhẹ, chỉ kiểm tra process có đang nhận được HTTP request hay không, không bao giờ gọi service phụ thuộc.
  - `/ready`: Kiểm tra toàn bộ dependencies bắt buộc (`mandatory=True`) như Kafka, Delta, Qdrant.
  - Chế độ **Degraded**: Nếu thành phần không bắt buộc (ví dụ vLLM chưa có GPU hoặc Feast cache lỗi), hệ thống vẫn trả về `degraded` thay vì sập toàn bộ dịch vụ, cho phép fallback sang cache hoặc câu trả lời tĩnh.
- **Ưu điểm:** Ngăn chặn hiện tượng Kubernetes restart pod liên tục theo vòng lặp (cascading failure / restart storms) khi một service phụ thuộc tạm thời không khả dụng.

---

## 3. Khoảng Cách Lên Môi Trường Sản Xuất (Production Gaps & Remediation)

Mặc dù kiến trúc lab đã đáp ứng đầy đủ 10 điểm kết nối, khi triển khai vào môi trường Production quy mô lớn cần giải quyết các khoảng cách sau:

| Vấn đề (Production Gap) | Thực trạng trong Lab | Giải pháp Production Chuẩn |
|---|---|---|
| **Độ sẵn sàng cao của Kafka** | 1 broker Kafka, replication factor = 1, in-sync replicas (ISR) = 1 | Cụm Kafka đa node (tối thiểu 3 brokers trải trên 3 Availability Zones), `min.insync.replicas = 2`, bật KRaft quorum phân tán. |
| **Quản lý Delta Lake Compaction** | Ghi nhiều batch nhỏ tạo ra nhiều parquet files nhỏ (small file problem) | Triển khai job Airflow định kỳ chạy `OPTIMIZE ... ZORDER BY` và `VACUUM` để dọn file rác và tối ưu tốc độ đọc. |
| **Bảo mật & Quản lý Định danh** | Plaintext gRPC/HTTP nội bộ, không có TLS nội vùng container | Kích hoạt mTLS thông qua Service Mesh (Istio / Linkerd), tích hợp Vault hoặc AWS Secrets Manager cho API keys. |
| **Xác thực vLLM & Auto-scaling** | Một endpoint vLLM tĩnh | Cụm vLLM chạy trên Kubernetes với KEDA (Kubernetes Event-driven Autoscaling) scale theo số lượng token request / queue depth. |
| **Giám sát SLO & Alerting** | Prometheus gom metric nhưng chưa có PagerDuty webhook | Thiết lập SLOs (99.9% availability, P95 latency < 500ms), cấu hình Alertmanager bắn thông báo tự động khi burn rate vượt ngưỡng. |

---

## 4. Kịch Bản Ứng Phó Sự Cố (Failure & Recovery Record)

### Kịch bản kiểm thử: Mất kết nối vLLM Serving (Optional Dependency Failure)
1. **Dự đoán tín hiệu (Hypothesis):**
   - Liveness probe `/health` vẫn trả về `HTTP 200`.
   - Readiness probe `/ready` chuyển từ `ready` sang `degraded`.
   - Metric `lab28_component_ready{component="vllm"}` chuyển về `0`.
   - Các request ingestion vào Kafka và truy vấn dữ liệu Delta không bị gián đoạn.
2. **Tiêm sự cố (Failure Injection):** Tắt service vLLM hoặc cấu hình sai URL mock.
3. **Quan sát thực tế:**
   - Hệ thống phát hiện lỗi ở dependency không bắt buộc, hàm `readiness_status` trả về trạng thái `"degraded"`.
   - Envoy Gateway vẫn định tuyến request, hệ thống trả fallback response rõ ràng, không sập pod.
4. **Khôi phục & Chứng minh không mất dữ liệu:**
   - Khởi động lại service vLLM.
   - Trạng thái `/ready` tự động phục hồi về `"ready"`.
   - Kiểm tra transaction log của Delta và offset của Kafka: không có event nào bị drop, không sinh ra duplicate row nhờ cơ chế `idempotency_key`.

---

## 5. Quy Trình GitOps & Rollback

- **Hạ tầng khai báo:** Khai báo toàn bộ hạ tầng bằng K8s manifests trong `deploy/kubernetes/base` và `gitops/application.yaml`.
- **Nguyên tắc bất biến:** Không dùng tag `:latest` cho container image; bắt buộc ghim commit hash hoặc semantic version (`v3.0.0`).
- **Phát hiện Drift:** Argo CD liên tục đối chiếu giữa Live State trên cluster và Desired State trong Git repo. Khi có sự can thiệp thủ công (imperative changes), Argo CD sẽ cảnh báo `OutOfSync` hoặc tự động áp dụng `Self-Heal`.
- **Rollback quy trình:**
  1. Rollback mô hình AI: Chuyển alias `@champion` trong MLflow Model Registry về version ổn định trước đó mà không cần rebuild container.
  2. Rollback ứng dụng: Git revert commit cấu hình K8s, Argo CD đồng bộ lại state chỉ trong vài giây.

---

## 6. Phân Công Trách Nhiệm & Đóng Góp (Team Roles & Ownership)

- **Học viên thực hiện:** **Xuân Thế Độ** (Mã học viên: **01847**)
- **Hình thức thực hiện:** Cá nhân (Đảm nhiệm toàn bộ 5 nhóm vai trò và 10 integration boundaries của hệ thống):

1. **Ingestion & Orchestration (IP01, IP02):**
   - Chịu trách nhiệm về Kafka topic schemas, producer header propagation (`traceparent`, `idempotency-key`), Airflow pipeline scheduling, DLQ configuration.
2. **Data & ML Engineering (IP03, IP04, IP06):**
   - Chịu trách nhiệm về Delta MERGE deduplication logic, Feast feature repository & materialization, MLflow model tracking & champion promotion/rollback.
3. **Serving & Retrieval (IP05, IP07):**
   - Chịu trách nhiệm về Qdrant deterministic vector indexing, vLLM client integration, degraded mode fallback, latency budget monitoring.
4. **Platform & Observability (IP08, IP09, IP10):**
   - Chịu trách nhiệm về Envoy Gateway rate limiting, OpenTelemetry distributed trace continuity, Prometheus metrics/alerts, Kubernetes/GitOps manifests.
5. **Incident Commander / Presenter:**
   - Điều phối kịch bản demo, chuẩn bị bằng chứng (evidence pack), giải thích trade-offs và phản biện Q&A trước hội đồng.
