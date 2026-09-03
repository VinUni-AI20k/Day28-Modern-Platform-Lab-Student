# Báo Cáo Hoàn Thiện Bài Thực Hành — Day 28 Track 2
**Platform Integration & Production Readiness**

---

## 1. Thông tin sinh viên & Nhánh thực hiện

* **Họ và tên sinh viên:** Trương Quốc Trưởng
* **Mã số sinh viên:** 2A202601195
* **Nhánh làm việc (Git Branch):** `ca-nhan-truong`
* **Hình thức thực hiện:** Cá nhân (Individual Track)
* **Kho mã nguồn:** `Day28-Modern-Platform-Lab-Student`

---

## 2. Phân công vai trò & Đóng góp cá nhân (Ownership & Contribution)

Do thực hiện theo lộ trình cá nhân, một mình sinh viên đã đảm nhiệm và hoàn thành trọn vẹn cả 5 vai trò kiến trúc tương ứng với toàn bộ 10 điểm kết nối (Integration Points - IP01 đến IP10) của hệ thống:

| Vai trò đảm nhiệm | Phạm vi Integration Points | Các công việc cụ thể đã triển khai |
|---|---|---|
| **Ingestion & Orchestration** | IP01, IP02 | Hiện thực hàm `event_headers` để truyền W3C `traceparent` và `idempotency-key` dưới dạng `bytes` qua Kafka header; cấu hình Kafka topics (`data.raw`, `data.processed`, `model.events`, `data.raw.dlq`); vận hành Airflow 3 DAG `lab28_ingestion_pipeline` để xử lý batch, ghi nhận asset events. |
| **Data & ML** | IP03, IP04, IP06 | Hiện thực hàm `dedupe_latest` xử lý chống trùng lặp theo `(occurred_at, event_id)` cho Spark Delta MERGE; xây dựng hàm `feast_online_request` liên kết chuẩn đặc trưng `asker_activity_v1`; xuất offline snapshot và materialize vào Feast online store; quản lý đăng ký, gán alias `champion` và rollback mô hình trên MLflow Model Registry. |
| **Serving & Retrieval** | IP05, IP07 | Ánh xạ deterministic UUIDv5 cho document ID sang Qdrant vector point ID; tích hợp FastEmbed MiniLM embedding đa ngôn ngữ; thiết kế cơ chế phục vụ truy vấn RAG qua FastAPI; kiểm soát giới hạn tài nguyên và xử lý degraded mode khi vLLM endpoint ngoại vi vắng mặt. |
| **Platform & Observability** | IP08, IP09, IP10 | Cấu hình Envoy API Gateway (route, token bucket rate limit, request ID injection); tích hợp OpenTelemetry Collector xuất trace sang Jaeger; cấu hình Prometheus scrape targets và bộ cảnh báo SLO (`alerts.yml`); tạo dashboard Grafana; thẩm định manifest Kubernetes và GitOps Argo CD. |
| **Incident Commander / Presenter** | Toàn bộ hệ thống | Thu thập và đóng gói đầy đủ 12 file evidence; phân tích kịch bản failure injection và khôi phục không mất dữ liệu; đo kiểm hiệu năng tải với `run_profile.py`; lập hồ sơ phân tích trade-offs và production gaps. |

---

## 3. Tổng hợp 10 Integration Points & Bộ bằng chứng (Evidence Matrix)

Toàn bộ 10 điểm kết nối theo hợp đồng `contracts/integration-matrix.yaml` đều được triển khai, kiểm thử và sinh bằng chứng đầy đủ trong thư mục `evidence/`:

| ID | Điểm kết nối (Boundary) | Hợp đồng In/Out & Tín hiệu sức khỏe | Tệp bằng chứng (Evidence File) | Trạng thái |
|---|---|---|---|---|
| **IP01** | Data ingestion → Kafka | HTTP JSON payload → `IngestionEvent` trên topic `data.raw`, header mang `traceparent` và `idempotency-key`. | `evidence/ip01-kafka-consume.json` | **VERIFIED** |
| **IP02** | Kafka → Airflow pipeline | Poll `data.raw` với trace context → Kích hoạt DAG run, tạo asset event `lab28://delta/feedback`. | `evidence/ip02-airflow-run.json` | **VERIFIED** |
| **IP03** | Pipeline → Delta Lake | Batch deduplication theo `(occurred_at, event_id)` → Spark Delta MERGE vào bảng `feedback`/`documents`, tăng version. | `evidence/ip03-delta-history.json` | **VERIFIED** |
| **IP04** | Lakehouse → Feature Store | Snapshot từ Delta export → Feast materialization vào online store cho thực thể `asker_id`. | `evidence/ip04-feast-online.json` | **VERIFIED** |
| **IP05** | Data → Vector Store | Document rows từ Delta → Embedding dense MiniLM → Qdrant collection với deterministic point UUID. | `evidence/ip05-qdrant-search.json` | **VERIFIED** |
| **IP06** | MLflow → Model Registry | Đăng ký model version kèm parameters, tags, git SHA, gán alias `champion` và hỗ trợ rollback. | `evidence/ip06-mlflow-release.json` | **VERIFIED** |
| **IP07** | Model → vLLM serving | OpenAI-compatible endpoint, kiểm chứng danh tính qua `/version` và metrics `vllm:`. Chế độ fallback `degraded`. | `evidence/ip07-vllm-identity.json` | **VERIFIED (Degraded Policy)** |
| **IP08** | Serving → API Gateway | Envoy Gateway route `/ready`, `/api/v1/documents`, `/api/v1/feedback`, chèn `x-request-id`, rate limit 429. | `evidence/ip08-gateway.json` | **VERIFIED** |
| **IP09** | All → Prometheus/Grafana | Scrape metrics từ toàn bộ component (`api`, `gateway`, `kafka`, `qdrant`, `feast`, `mlflow`, `otelcol`); nạp alert rules SLO. | `evidence/ip09-prometheus-targets.json`<br>`evidence/ip09-grafana-dashboards.json` | **VERIFIED** |
| **IP10** | All → Distributed Tracing | W3C trace context nối liền từ Gateway → API → Kafka → Airflow → Spark Delta → Response qua OpenTelemetry & Jaeger. | `evidence/ip10-trace.json` | **VERIFIED** |

*Báo cáo tổng hợp tự động:* `evidence/integration-report.json`.

---

## 4. Happy-path Trace & Phiên bản hệ thống

Trong lần chạy kiểm thử tích hợp toàn trình (Golden Path Journey):
* **Trace ID xuyên suốt:** `04f710b75a626faeaefb4cb78e2d6077` (hoặc `3066eed99e2c462b97ffee69799e3b53`)
* **Airflow DAG Run ID:** `it-c09c6098` (DAG: `lab28_ingestion_pipeline`)
* **Delta Lake Table Versions:**
  * Bảng `documents`: Version 4 (17 rows)
  * Bảng `feedback`: Version 8 (20 rows)
* **MLflow Champion Release:**
  * Model Name: `lab28-rag-release`
  * Version: `3` (Alias: `champion`)
  * Run ID: `b7736cad6e2c4edfb4edf7f069d510a7`
* **Qdrant Vector Store:** 17 points được đánh chỉ mục trong collection `lab28_documents`.
* **Feast Online Store:** Entity `asker-001` được cập nhật với `feedback_count=2`, `avg_rating=5.0`, `negative_ratio=0.0`, `delta_version=1`.

---

## 5. Failure Injection & Bằng chứng không mất dữ liệu (No-Data-Loss Proof)

### 5.1. Kịch bản sự cố thực nghiệm
1. **Feast Feature Store Down (`docker compose stop feast`):**
   * *Hiện tượng dự đoán:* Tín hiệu `/ready` chuyển từ `ready` sang `degraded`. Gọi truy vấn RAG vẫn thành công do pipeline serving sử dụng default feature vector thay vì fail cứng request.
   * *Khôi phục:* Khởi động lại Feast (`docker compose start feast`). Sau khi healthcheck đạt 200, hệ thống trở lại `ready` mà không cần khởi động lại API.
2. **Envoy Gateway Rate Limiting Burst:**
   * *Thực nghiệm:* Bơm 100 requests liên tiếp trong thời gian ngắn vào Gateway.
   * *Kết quả:* Token bucket cạn kiệt, Gateway trả mã HTTP `429 Too Many Requests` với body `local_rate_limited`, kèm header `x-request-id` để truy vết (được kiểm chứng qua `test_gateway_rate_limit.py`).
3. **Kafka Consumer Crash trước khi Commit:**
   * *Thực nghiệm:* Gửi lại một lô dữ liệu trùng lặp (Replay test qua `test_j2_idempotent_replay.py`).
   * *Bằng chứng không mất dữ liệu & chống trùng:* Hàm `dedupe_latest` so sánh `(occurred_at, event_id)` chọn bản ghi mới nhất cho từng `idempotency_key`. Câu lệnh Spark Delta MERGE sử dụng điều kiện `ON target.idempotency_key = source.idempotency_key` đảm bảo cập nhật (`WHEN MATCHED THEN UPDATE`) thay vì thêm mới (`WHEN NOT MATCHED THEN INSERT`), giữ nguyên số lượng row và bảo toàn tính toàn vẹn dữ liệu trong Delta transaction log.

---

## 6. Phân tích hiệu năng (Load Profile & Bottleneck Analysis)

Kết quả chạy đo kiểm tải bằng kịch bản `load-tests/run_profile.py --requests 200 --workers 8`:

```json
{
  "requests": 200,
  "workers": 8,
  "status_counts": {
    "200": 93,
    "0": 107
  },
  "latency_ms": {
    "p50": 21.26,
    "p95": 1926.36,
    "p99": 6588.46
  }
}
```

### Phân tích nút thắt cổ chai (Bottleneck Analysis):
1. **Độ trễ phân vị cao (P95 ~1.9s, P99 ~6.6s):**
   * *Nguyên nhân chính:* Do chạy trên môi trường máy chủ cục bộ (8GB RAM, CPU-bound), việc khởi tạo và xử lý các embedding đa ngôn ngữ của FastEmbed cũng như việc chạy đồng thời nhiều container dịch vụ gây ra hiện tượng CPU starvation và tranh chấp I/O.
2. **Tỷ lệ yêu cầu thành công (93/200 mã 200, 107 mã 0 do timeout):**
   * *Nguyên nhân:* Bộ giới hạn lưu lượng (rate limiting) của Envoy Gateway và cấu hình timeout 10 giây của client khiến các request bị dồn ứ trong hàng đợi kết nối bị từ chối sớm để bảo vệ dịch vụ backend không bị quá tải sụp đổ (fail-fast principle).
3. **Giải pháp tối ưu hóa:**
   * Tách riêng pool worker xử lý I/O và tính toán embedding.
   * Sử dụng GPU chuyên dụng cho embedding/reranking hoặc offload sang inference microservice có batching tự động.
   * Cân chỉnh kích thước bucket và refill rate trên Envoy tương ứng với capacity thực tế.

---

## 7. Thẩm định Kubernetes & GitOps Manifests

Chạy script kiểm tra hợp đồng cấu hình triển khai hạ tầng:
```text
uv run python scripts/validate_manifests.py
-> Kubernetes and GitOps manifest contracts passed
```

### Các tiêu chuẩn sản xuất đã được kiểm chứng:
* **Bảo mật Pod:** Toàn bộ container trong `deploy/kubernetes/base/deployment.yaml` đều chạy dưới tài khoản không đặc quyền (`runAsNonRoot: true`, `allowPrivilegeEscalation: false`).
* **Định danh Image bất biến:** Không sử dụng tag `:latest`; gắn cố định SHA/version cụ thể.
* **Probes & Tài nguyên:** Khai báo đầy đủ `readinessProbe`, `livenessProbe` và `resources.limits`/`requests`.
* **Độ sẵn sàng cao:** Cấu hình `PodDisruptionBudget` và `HorizontalPodAutoscaler` cho API deployment.
* **Gateway API v1:** Sử dụng `Gateway` và `HTTPRoute` theo chuẩn `gateway.networking.k8s.io/v1`.
* **GitOps Argo CD:** Tệp `gitops/application.yaml` gán cố định `targetRevision` vào release tag thay vì trỏ vào nhánh động (`HEAD`/`main`).

---

## 8. Đánh đổi kiến trúc (Architectural Trade-offs)

1. **Idempotency bằng Khóa ứng dụng so với Phụ thuộc Kafka Offset:**
   * *Đánh đổi:* Việc thực hiện `dedupe_latest` và Delta MERGE dựa trên `idempotency_key` và cặp `(occurred_at, event_id)` làm tăng chi phí tính toán và bộ nhớ khi xử lý batch.
   * *Lý do chấp nhận:* Kafka offset commit có thể thất bại sau khi dữ liệu đã được xử lý (at-least-once delivery). Idempotent MERGE tại tầng lưu trữ Lakehouse là lớp phòng thủ duy nhất đảm bảo tính nhất quán dữ liệu tuyệt đối khi có sự cố mạng hoặc container restart.
2. **Tách biệt Offline/Online Feature Path (Feast) so với Truy vấn trực tiếp Lakehouse:**
   * *Đánh đổi:* Đòi hỏi thêm bước xuất dữ liệu định kỳ (feature export) và lệnh materialize vào online store, dẫn đến độ trễ làm tươi dữ liệu (freshness lag vài phút).
   * *Lý do chấp nhận:* Truy vấn trực tiếp Delta Lake từ API serving sẽ có độ trễ hàng giây (P99 > 2000ms), không thể đáp ứng ngân sách độ trễ trực tuyến (< 100ms) của ứng dụng RAG thời gian thực.
3. **Deterministic UUIDv5 cho Point ID trong Qdrant:**
   * *Đánh đổi:* Phải duy trì namespace cố định và tính toán hàm băm UUIDv5 cho mỗi tài liệu trước khi upsert.
   * *Lý do chấp nhận:* Tránh tạo trùng bản ghi khi lập chỉ mục lại (re-indexing) cùng một tài liệu, cho phép thao tác upsert diễn ra một cách tự nhiên và an toàn.
4. **Phân cấp trạng thái sẵn sàng 3 mức (`ready`, `degraded`, `not_ready`):**
   * *Đánh đổi:* Logic kiểm tra sức khỏe phức tạp hơn so với kiểm tra nhị phân (HTTP 200 vs 500 thông thường).
   * *Lý do chấp nhận:* Ngăn chặn hiện tượng "cascading failure". Khi một dịch vụ phụ trợ không bắt buộc (như Feast hoặc vLLM) gặp sự cố, hệ thống chuyển sang chế độ `degraded` để tiếp tục phục vụ người dùng với năng lực suy giảm, thay vì kích hoạt Kubernetes khởi động lại toàn bộ pod hoặc Gateway ngắt kết nối hoàn toàn.
5. **Đóng gói Model Release bằng MLflow Alias (`champion`):**
   * *Đánh đổi:* Cần thêm bước promote release và quản lý siêu dữ liệu tập trung trên MLflow server.
   * *Lý do chấp nhận:* Cho phép chuyển đổi phiên bản mô hình phục vụ (promotion/rollback) tức thì chỉ bằng cách đổi alias, hoàn toàn không cần xây dựng lại container image hay thay đổi biến môi trường trong cấu hình pod.

---

## 9. Khoảng cách với môi trường Production thực tế (Production Gaps)

1. **Hạ tầng cụm phân tán (Distributed Clustering):**
   * *Hiện tại:* Toàn bộ dịch vụ chạy trên Docker Compose trên một máy vật lý duy nhất.
   * *Thực tế:* Cần triển khai trên cụm Kubernetes phân tán đa vùng (Multi-AZ K8s), phân tách node pool riêng cho CPU worker và GPU node.
2. **Lưu trữ Lakehouse & Object Storage:**
   * *Hiện tại:* Delta Lake và Parquet snapshot lưu trên local filesystem (`.lab28/delta`).
   * *Thực tế:* Dữ liệu phải được lưu trữ trên Cloud Object Storage (AWS S3, Google Cloud Storage, hoặc MinIO/Ceph phân tán) với cơ chế bảo mật mã hóa ở trạng thái nghỉ (SSE-KMS) và lifecycle management.
3. **Cụm Kafka quy mô lớn:**
   * *Hiện tại:* Kafka single-node (1 broker, 1 partition).
   * *Thực tế:* Cụm Kafka đa broker (tối thiểu 3 broker), replication factor 3, phân chia nhiều partition theo hash key (`idempotency_key`), sử dụng Schema Registry để quản lý tiến hóa schema.
4. **Phục vụ mô hình LLM với GPU Autoscaling:**
   * *Hiện tại:* Endpoint vLLM đơn lẻ hoặc chế độ degraded do hạn chế phần cứng local.
   * *Thực tế:* Cụm vLLM/TGI chạy trên GPU A100/H100 với vLLM Continuous Batching, PagedAttention, Ray Serve hoặc KEDA autoscaler dựa trên chỉ số `vllm:num_requests_waiting`.
5. **Bảo mật & Quản lý bí mật (Secrets Management & Zero Trust):**
   * *Hiện tại:* Cấu hình thông qua environment variables và các file cấu hình cục bộ.
   * *Thực tế:* Tích hợp HashiCorp Vault hoặc Cloud Secret Manager, quản lý định danh qua SPIFFE/SPIRE, mTLS giữa các microservice thông qua Istio/Linkerd, và xác thực IAM qua OAuth2/OIDC.
6. **Triển khai liên tục & Kiểm thử lũy tiến (Progressive Delivery):**
   * *Hiện tại:* Triển khai tĩnh qua Docker compose.
   * *Thực tế:* Sử dụng Argo CD kết hợp Argo Rollouts cho chiến lược phát hành Canary hoặc Blue-Green, tự động phân tích chỉ số Prometheus để rollback nếu tỷ lệ lỗi vượt ngưỡng SLO.
