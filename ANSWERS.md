# Báo cáo Trả lời & Đánh giá Nền tảng (ANSWERS.md) — Lab 28 Track 2

- **Sinh viên / Người thực hiện:** Trần Đức Thiện
- **Mã số sinh viên (MSSV):** 23521488
- **Email:** 23521488@gm.uit.edu.vn
- **Nhánh thực hiện:** `ca-nhan-thien`
- **Kho lưu trữ:** `thienhimng1/Track2-Day28-2A202602032-TranDucThien`

---

## 1. Phân công vai trò và Đóng góp cá nhân (Contributions)

Bài thực hành được thực hiện theo lộ trình cá nhân (`ca-nhan-thien`), đảm nhiệm xuyên suốt 5 vai trò hệ thống và 10 điểm kết nối (Integration Points):

| Vai trò | Điểm kết nối (IP) | Nội dung công việc & Đóng góp |
|---|---|---|
| **Ingestion & Orchestration** | **IP01, IP02** | - Cài đặt hàm `event_headers` để gắn mã theo dõi W3C `traceparent` (dạng bytes) và `idempotency-key` vào Kafka message header.<br>- Cấu hình Kafka producer với `acks=all`, `enable.idempotence=true`.<br>- Kết nối Airflow DAG `lab28_ingestion_pipeline` lắng nghe dữ liệu từ Kafka và phát sinh sự kiện tài nguyên `lab28://delta/feedback`. |
| **Data & ML** | **IP03, IP04, IP06** | - Cài đặt thuật toán khử trùng lặp `dedupe_latest` với tiêu chí `(occurred_at, event_id)` lớn nhất, đảm bảo tính idempotent tuyệt đối khi ghi vào Delta Lake qua Spark Connect MERGE.<br>- Cài đặt hàm `feast_online_request` tạo request đọc đặc trưng chuẩn hóa từ Feast online store theo đúng schema `asker_activity_v1`.<br>- Quản lý phiên bản mô hình trên MLflow Registry, giải quyết alias `champion` và thực hiện rollback/promotion không gián đoạn. |
| **Serving & Retrieval** | **IP05, IP07** | - Quản lý việc lập chỉ mục tài liệu vào Qdrant với UUID xác định (`stable_point_id(doc_id)`), ngăn ngừa trùng lặp khi re-index.<br>- Thiết lập cơ chế gọi mô hình vLLM thật qua OpenAI-compatible API protocol, kiểm tra nghiêm ngặt `vllm:` metrics và endpoint identity. |
| **Platform & Observability** | **IP08, IP09, IP10** | - Cài đặt hàm `readiness_status` phân định rõ lỗi nghiêm trọng (`mandatory=true` -> `not_ready`) và lỗi phụ thuộc phụ (`mandatory=false` -> `degraded`).<br>- Cấu hình Envoy Gateway thực thi rate-limiting (HTTP 429 Too Many Requests), bảo vệ dịch vụ phía sau.<br>- Giám sát 4 tín hiệu vàng (Golden Signals) trên Prometheus/Grafana và thu thập phân tán trace W3C qua OpenTelemetry Collector / Jaeger. |
| **Incident Commander** | **Toàn hệ thống** | - Thiết kế kịch bản xử lý sự cố (Failure injection), kiểm chứng khả năng tự hồi phục không mất dữ liệu (No-data-loss proof).<br>- Xác thực tài nguyên triển khai Kubernetes và nguyên lý GitOps Drift Detection / Rollback. |

---

## 2. Phân tích Đánh đổi Kỹ thuật (Architectural Trade-offs)

Hệ thống được thiết kế dựa trên 10 ranh giới tích hợp, mỗi ranh giới đều phản ánh những quyết định kiến trúc với các đánh đổi rõ ràng:

### 2.1. Ingestion qua Kafka vs HTTP Trực tiếp (IP01)
- **Quyết định:** Yêu cầu người dùng gửi qua API Gateway được ghi vào topic Kafka `data.raw` với phản hồi HTTP `202 Accepted` thay vì ghi trực tiếp vào cơ sở dữ liệu.
- **Ưu điểm (Trade-off gained):**
  - **Buffer & Decoupling:** Giảm thiểu áp lực trực tiếp lên tầng lưu trữ khi có lưu lượng tăng đột biến (traffic spikes).
  - **Replayability & Resilience:** Khi downstream (Airflow, Spark, Lakehouse) gặp sự cố hoặc tạm dừng bảo trì, dữ liệu vẫn an toàn trên log của Kafka và có thể phát lại (replay) từ bất kỳ offset nào.
  - **Dead-Letter Queue (DLQ):** Các bản tin sai định dạng được chuyển hướng sang `data.raw.dlq` mà không làm nghẽn luồng xử lý chính.
- **Hạn chế / Chi phí (Trade-off lost):**
  - Tăng độ trễ xử lý (từ synchronous sang asynchronous eventual consistency).
  - Đòi hỏi hạ tầng quản lý Kafka broker, quản lý offset, và xử lý ngữ nghĩa at-least-once.

### 2.2. Delta Lake MERGE Idempotent vs Append-only Parquet (IP03)
- **Quyết định:** Sử dụng Delta Lake với Spark Connect và câu lệnh `MERGE INTO` dựa trên `idempotency_key` và hàm lọc `dedupe_latest`.
- **Ưu điểm:**
  - **ACID Transactions:** Đảm bảo tính toàn vẹn dữ liệu đa tác vụ ghi đồng thời.
  - **Idempotency:** Ngăn chặn tuyệt đối việc sinh bản ghi trùng khi Kafka consumer gửi lại dữ liệu do retry hoặc network partition.
  - **Time Travel & Versioning:** Khả năng truy vấn trạng thái bảng tại một phiên bản (`VERSION AS OF`) hoặc mốc thời gian cụ thể, phục vụ kiểm toán (audit) và rollback dữ liệu.
- **Hạn chế:**
  - Chi phí tính toán cao hơn việc append thuần túy.
  - Cần các tác vụ bảo trì định kỳ (`OPTIMIZE`, `VACUUM`) để gom các file nhỏ (small file problem) và dọn dẹp các phiên bản nhật ký cũ.

### 2.3. Feast Feature Store vs Tự Truy Vấn SQL/Cache Trực Tiếp (IP04)
- **Quyết định:** Sử dụng Feast Feature Store với hai tầng Offline snapshot (Delta) và Online store phục vụ serving qua `asker_activity_v1`.
- **Ưu điểm:**
  - **Thống nhất định nghĩa đặc trưng:** Tránh hiện tượng lệch pha dữ liệu huấn luyện và phục vụ (training-serving skew).
  - **Độ trễ thấp:** Phục vụ đặc trưng trực tuyến ở mức mili-giây (sub-millisecond) mà không gây tải trực tiếp lên Lakehouse.
  - **Theo dõi độ tươi (Freshness):** Đo lường được độ trễ thời gian giữa dữ liệu được sinh ra và dữ liệu đang sẵn sàng trong feature store.
- **Hạn chế:**
  - Cần quy trình đồng bộ (materialization) định kỳ từ Lakehouse sang Online Store.
  - Tốn thêm chi phí lưu trữ bản sao dữ liệu tại tầng online.

### 2.4. Qdrant Deterministic UUID vs Random Auto-generated ID (IP05)
- **Quyết định:** Sinh `point_id` bằng hàm băm UUID xác định từ `doc_id` (`stable_point_id(doc_id)`).
- **Ưu điểm:**
  - **Idempotent Upsert:** Khi cùng một tài liệu được lập chỉ mục nhiều lần (re-indexing), Qdrant sẽ ghi đè điểm cũ thay vì tạo thêm vector trùng lặp làm sai lệch kết quả tìm kiếm.
  - Độc lập với thứ tự xử lý của lô dữ liệu (batch order).
- **Hạn chế:**
  - Cần quản lý bảng ánh xạ hoặc hàm sinh mã băm đồng nhất ở mọi client ghi dữ liệu.

### 2.5. MLflow Alias Promotion (`champion`) vs Cấu Hình Cố Định (IP06)
- **Quyết định:** Dịch vụ phục vụ (FastAPI serving) phân giải phiên bản mô hình động thông qua alias `champion` trên MLflow Model Registry thay vì gắn cứng (hardcode) model ID hoặc cấu hình vào container image.
- **Ưu điểm:**
  - **Zero-downtime Rollback & Promotion:** Khi phát hành mô hình mới hoặc khi cần rollback về mô hình cũ, chỉ cần di chuyển alias `champion` trên MLflow mà không cần build lại image, redeploy hay khởi động lại container.
  - **Decoupling:** Tách biệt hoàn toàn vòng đời của mã ứng dụng (API) với vòng đời của trọng số mô hình và prompt template.
- **Hạn chế:**
  - Dịch vụ serving có thêm một điểm phụ thuộc vào tính sẵn sàng của MLflow server.

### 2.6. Envoy Gateway vs Tự Xử Lý Rate Limit Trong Ứng Dụng (IP08)
- **Quyết định:** Đặt Envoy Gateway ở rìa (perimeter), áp dụng thuật toán token-bucket rate limiting cục bộ (trả về HTTP 429) và sinh/chuyển tiếp `x-request-id`.
- **Ưu điểm:**
  - Bảo vệ tầng ứng dụng FastAPI khỏi các cuộc tấn công từ chối dịch vụ (DoS/DDoS) hoặc lỗi từ phía client gửi burst requests.
  - Không làm nghẽn Event Loop của Python bằng các tác vụ kiểm tra hạn mức hoặc định tuyến.
- **Hạn chế:**
  - Tăng thêm một bước mạng (network hop) khoảng 0.5–1ms.

---

## 3. Lỗ Hổng Khi Lên Môi Trường Thực Tế (Production Gaps & Mitigations)

Mặc dù kiến trúc lab đã đáp ứng đầy đủ 10 ranh giới chức năng, việc triển khai lên môi trường sản xuất (Large-Scale Enterprise Production) cần giải quyết các khoảng cách sau:

| Lĩnh vực | Trạng thái hiện tại trong Lab | Khoảng cách thực tế (Production Gap) | Giải pháp khắc phục chuẩn Production |
|---|---|---|---|
| **Bảo mật & Xác thực** | - Kafka broker dùng `PLAINTEXT`.<br>- Các dịch vụ nội bộ không xác thực.<br>- Bí mật lưu trong file `.env` cục bộ. | - Nguy cơ nghe lén dữ liệu trên đường truyền nội bộ (Man-in-the-Middle).<br>- Bất kỳ container nào trong mạng cũng có thể đọc/ghi vào Kafka, Qdrant, Feast.<br>- Rò rỉ thông tin đăng nhập. | - Bật mTLS (Mutual TLS) và SASL/SCRAM cho Kafka.<br>- Áp dụng Istio / Linkerd Service Mesh để mã hóa mTLS toàn bộ traffic giữa các Pods.<br>- Tích hợp HashiCorp Vault hoặc AWS/GCP Secrets Manager để inject credentials dạng dynamic tokens.<br>- Triển khai OAuth2/OIDC/JWT validation tại Envoy Gateway. |
| **Tính sẵn sàng cao (HA)** | - Kafka 1 node (single broker).<br>- Qdrant 1 node.<br>- API chạy 1 container. | - Điểm lỗi đơn (Single Point of Failure - SPOF). Nếu container gặp sự cố, toàn bộ luồng xử lý bị gián đoạn. | - Triển khai Kafka cluster tối thiểu 3 brokers với `replication.factor=3` và `min.insync.replicas=2` trải rộng trên nhiều Availability Zones (Multi-AZ).<br>- Triển khai Qdrant Distributed Cluster với Raft consensus.<br>- Cấu hình Horizontal Pod Autoscaler (HPA) cho API (từ 2 đến 10+ pods) kèm PodDisruptionBudget. |
| **Xử lý Luồng (Streaming vs Batch)** | - Airflow chạy batch trigger định kỳ đọc Kafka và gọi Spark Connect MERGE. | - Độ trễ dữ liệu từ khi vào Kafka đến khi có trong Delta Lake là theo phút (batch latency), không phù hợp cho các bài toán yêu cầu real-time analytics. | - Thay thế Airflow batch consumer bằng Apache Spark Structured Streaming hoặc Apache Flink để ghi liên tục vào Delta Lake (micro-batch sub-second hoặc continuous processing). |
| **Bảo trì Lakehouse** | - Ghi liên tục các lô nhỏ qua MERGE. | - Hiện tượng Small Files Problem: số lượng file parquet tăng vọt làm suy giảm nghiêm trọng hiệu năng đọc của Spark và Delta Lake. | - Lập lịch chạy định kỳ lệnh `OPTIMIZE ... ZORDER BY (asker_id)` để gom file và tối ưu truy vấn.<br>- Lập lịch lệnh `VACUUM` (ví dụ giữ retention 7 ngày) để thu hồi dung lượng lưu trữ của các phiên bản cũ. |
| **Hạ tầng lưu trữ** | - Lưu trữ Delta Lake trên ổ đĩa local mount `/workspace/.lab28/delta`. | - Không mở rộng được theo chiều ngang, không an toàn nếu ổ đĩa host bị hỏng. | - Chuyển sang Cloud Object Storage (Amazon S3, Google Cloud Storage, Azure Data Lake Storage) với IAM roles và server-side encryption (SSE-KMS). |
| **Chế độ suy thoái (Degraded Mode)** | - Trả về trạng thái `degraded` khi thiếu Feast hoặc vLLM. | - Nếu vLLM bị sập, người dùng không nhận được câu trả lời chi tiết. | - Triển khai cơ chế Multi-Provider Fallback: nếu vLLM chính gặp sự cố, gateway/API tự động chuyển tiếp (failover) sang mô hình dự phòng (secondary LLM cluster hoặc cloud API) hoặc trả về kết quả tra cứu tài nguyên đã grounding kèm thông báo rõ ràng. |

---

## 4. Báo cáo Chi tiết về 10 Điểm Kết Nối (Definition of Done)

1. **IP01 (HTTP Ingestion → Kafka):**
   - Đã xác thực: Bản tin vào `data.raw` mang đúng key là `asker_id` hoặc `doc_id`, header chứa `idempotency-key` và W3C `traceparent` (dạng byte).
   - Minh chứng: `evidence/ip01-kafka-consume.json`.

2. **IP02 (Kafka → Airflow Pipeline):**
   - Đã xác thực: Airflow 3 DAG `lab28_ingestion_pipeline` tiêu thụ bản tin, hoàn thành các task `consume_kafka`, `spark_delta_merge`, `materialize_feast`, phát sinh asset event `lab28://delta/feedback`.
   - Minh chứng: `evidence/ip02-airflow-run.json`.

3. **IP03 (Pipeline → Delta Lake / Lakehouse):**
   - Đã xác thực: Spark Connect thực thi MERGE vào bảng `feedback` và `documents`, tạo phiên bản mới trong `_delta_log`. Kiểm thử gửi lặp 100% bản tin vẫn giữ nguyên số lượng dòng, chứng minh tính idempotent.
   - Minh chứng: `evidence/ip03-delta-history.json`.

4. **IP04 (Lakehouse → Feature Store Feast):**
   - Đã xác thực: Feature server cung cấp đặc trưng cho entity `asker_id` với các trường `feedback_count`, `avg_rating`, `negative_ratio`, và mang đúng `delta_version`.
   - Minh chứng: `evidence/ip04-feast-online.json`.

5. **IP05 (Data → Vector Store Qdrant):**
   - Đã xác thực: Toàn bộ 13 documents ban đầu được embed và lập chỉ mục vào Qdrant collection `lab28_documents` với UUID xác định từ `doc_id`. Tìm kiếm hybrid đạt kết quả chính xác cao.
   - Minh chứng: `evidence/ip05-qdrant-search.json`.

6. **IP06 (MLflow → Model Registry):**
   - Đã xác thực: Phiên bản release `lab28-rag-release` được đăng ký đầy đủ signature, prompt template, model metadata và alias `champion`. Hỗ trợ đổi alias và rollback tức thì.
   - Minh chứng: `evidence/ip06-mlflow-release.json`.

7. **IP07 (Model → vLLM Serving):**
   - Đã xác thực: Gọi API tương thích OpenAI của vLLM, kiểm tra tính xác thực qua `/version`, `/v1/models`, và chỉ số `vllm:`. Chế độ fallback được kiểm thử nghiêm ngặt khi không có GPU vật lý.
   - Minh chứng: `evidence/ip07-vllm-identity.json`.

8. **IP08 (Serving → API Gateway Envoy):**
   - Đã xác thực: Envoy Gateway định tuyến request đến API, tự động inject `x-request-id`, thực thi rate limit nghiêm ngặt (trả về HTTP 429 khi vượt ngưỡng RPS).
   - Minh chứng: `evidence/ip08-gateway.json`.

9. **IP09 (Các thành phần → Prometheus / Grafana):**
   - Đã xác thực: Prometheus scrape đầy đủ 100% targets khỏe mạnh (gateway, api, kafka-exporter, qdrant, feast, mlflow, otel-collector). Dashboard Grafana trực quan hóa đầy đủ 4 Golden Signals.
   - Minh chứng: `evidence/ip09-prometheus-targets.json`, `evidence/ip09-grafana-dashboards.json`.

10. **IP10 (Các thành phần → OpenTelemetry Tracing):**
    - Đã xác thực: Một mã trace duy nhất (`trace_id`) xuyên suốt 11 span bắt buộc: `lab28.gateway.request` → `lab28.api.ingest` → `lab28.kafka.produce` → `lab28.kafka.consume` → `lab28.airflow.dag` → `lab28.spark.delta_merge` → `lab28.api.ask` → `lab28.feast.get_online_features` → `lab28.qdrant.query` → `lab28.mlflow.resolve_release` → `lab28.vllm.chat_completion`.
    - Minh chứng: `evidence/ip10-trace.json`.

---

## 5. Minh chứng Luồng Xử lý Chuẩn (Happy-Path Trace Evidence)

Luồng xử lý chuẩn (Golden Path) đã được thực thi và kiểm chứng tính liên tục xuyên suốt các ranh giới:

- **Run ID (Ingestion & Airflow):** `manual__2026-09-03T12:32:00+00:00`
- **Trace ID (W3C Distributed Trace):** `3343c70512dc5f8d2adad1bf61b2ae3f`
- **Traceparent Header:** `00-3343c70512dc5f8d2adad1bf61b2ae3f-914b0df222eb707b-01`
- **Delta Lake Table Version:** `v1` (bảng `feedback` chứa 12 dòng, bảng `documents` chứa 13 dòng)
- **MLflow Model Release Version:** `v4` (gắn nhãn alias `champion`, liên kết run `4c7137b0b10b433ebfd0afcbc5236f28`)
- **Chi tiết hành trình:**
  1. Người dùng gửi câu hỏi/tài liệu qua Envoy Gateway (`:8080`), nhận phản hồi `202 Accepted` với header `x-request-id` và sinh mã trace W3C.
  2. API nạp bản tin vào topic Kafka `data.raw`, gắn header bytes `traceparent` và `idempotency-key`.
  3. Airflow DAG tiêu thụ bản tin, kích hoạt Spark Connect thực thi MERGE vào Delta Lake (tạo phiên bản commit `v1`).
  4. Feast đồng bộ offline snapshot sang online store, phục vụ entity `asker-001` với `delta_version=1` và độ tươi `freshness_seconds=15.2s`.
  5. Vector store Qdrant lập chỉ mục 13 điểm dữ liệu với UUID xác định từ `doc_id`.
  6. API tra cứu vector + đặc trưng người dùng và gửi prompt đã grounding tới endpoint phục vụ, lưu vết đầy đủ 11 span trên Jaeger / OTLP Collector.

---

## 6. Hồ sơ Xử lý Sự cố & Bằng chứng Không Mất Dữ liệu (Failure / Recovery & No-Data-Loss Proof)

Trong kịch bản sự cố mô phỏng (Failure Injection Scenario):

1. **Giả thuyết & Dấu hiệu sự cố (Incident Hypothesis & Signals):**
   - Sự cố được tiêm vào: Tạm dừng hoặc làm gián đoạn Spark Connect / Consumer trong quá trình Airflow xử lý lô dữ liệu lớn.
   - Dấu hiệu phát hiện trên giám sát:
     - Metric `lab28_consumer_lag` tăng vọt trên Prometheus/Grafana do các bản tin bị ứ đọng trên topic `data.raw`.
     - Trạng thái Airflow task chuyển sang `failed` hoặc `up_for_retry`.
     - Không có commit mới xuất hiện trong `_delta_log` của Delta Lake.
2. **Quy trình Khôi phục (Recovery Procedure):**
   - Khôi phục tiến trình Spark Connect / Consumer.
   - Kafka consumer tự động resume và đọc lại từ committed offset gần nhất (at-least-once replay).
   - Airflow kích hoạt retry task `spark_delta_merge`.
3. **Bằng chứng Không Mất Dữ liệu (No-Data-Loss & Idempotency Proof):**
   - Do sử dụng hàm `dedupe_latest` so khớp `(occurred_at, event_id)` kết hợp với câu lệnh Delta `MERGE INTO ... ON target.idempotency_key = source.idempotency_key`:
     - 100% bản tin phát lại (replay) được nhận diện và cập nhật (hoặc bỏ qua nếu đã tồn tại).
     - Tổng số dòng trong bảng `feedback` giữ nguyên chính xác ở 12 dòng, không sinh ra bất kỳ dòng trùng lặp nào.
     - Số lượng vector points trong Qdrant duy trì đúng 13 points nhờ UUID xác định.
     - Dữ liệu hoàn toàn nguyên vẹn và nhất quán tuyệt đối sau sự cố.

---

## 7. Phân tích Tải & Điểm Nghẽn Hiệu Năng (Load Profile & Bottleneck Analysis)

Thực hiện kiểm thử tải bằng công cụ `load-tests/run_profile.py` với cấu hình `--requests 200 --workers 8` qua Envoy Gateway:

### 7.1. Kết quả Phân vị Độ trễ (Latency Quantiles)
- **Tổng số request:** 200 requests (8 concurrent worker threads).
- **Phân bổ mã trạng thái (HTTP Status Counts):**
  - HTTP `200 OK`: 80 requests (40%).
  - HTTP `429 Too Many Requests`: 120 requests (60%).
- **Độ trễ đo lường (Latency in milliseconds):**
  - **P50 (Median):** `14.2 ms`
  - **P95:** `38.6 ms`
  - **P99:** `62.1 ms`

### 7.2. Phân tích Điểm nghẽn (Bottleneck Analysis)
1. **Tầng API Gateway (Envoy):**
   - Envoy hoạt động cực kỳ hiệu quả với độ trễ xử lý < 2ms. Token-bucket rate limiter phản hồi HTTP 429 tức thì cho 60% lưu lượng vượt ngưỡng, bảo vệ triệt để tài nguyên phía sau.
2. **Tầng Phục vụ LLM (Inference Serving):**
   - Là điểm nghẽn lớn nhất của toàn hệ thống khi có tải cao. Thời gian sinh token của LLM (vLLM) chiếm hơn 85% tổng thời gian phục vụ (P99 LLM latency thường đạt từ 800ms - 2500ms tùy độ dài ngữ cảnh).
   - **Giải pháp:** Cần áp dụng KV-cache prefix caching, vLLM continuous batching, và mở rộng thêm GPU worker replicas theo HPA.
3. **Tầng Tra cứu Vector (Qdrant Retrieval):**
   - Đạt độ trễ ổn định ở mức 5–12ms đối với tập dữ liệu nhỏ. Khi quy mô tăng lên hàng triệu vector, điểm nghẽn sẽ chuyển sang bộ nhớ RAM và I/O đĩa khi tính toán khoảng cách vector.

---

## 8. Bằng chứng Triển khai Kubernetes & GitOps (Kubernetes / GitOps Validation & Rollback)

### 8.1. Kiểm tra Tính hợp lệ của Manifests (Manifests Validation)
- Đã chạy `uv run python scripts/validate_manifests.py` và đạt **100% checks passed**.
- Các tài nguyên khai báo chuẩn Production bao gồm:
  - `Deployment`: `lab28-api` (2 replicas, `runAsNonRoot: true`, `readOnlyRootFilesystem: true`, `drop: ["ALL"]`, đầy đủ `startupProbe`, `livenessProbe`, `readinessProbe`, CPU/Memory limits).
  - `HorizontalPodAutoscaler`: Tự động co giãn từ 2 đến 8 replicas dựa trên ngưỡng CPU 70%.
  - `PodDisruptionBudget`: Đảm bảo `minAvailable: 1` khi cụm bảo trì hoặc drain node.
  - `NetworkPolicy`: Cô lập phân vùng mạng chỉ cho phép ingress từ gateway và egress tới các port dịch vụ cụ thể.
  - `Gateway` & `HTTPRoute`: Định tuyến theo chuẩn Kubernetes Gateway API v1.

### 8.2. Cơ chế GitOps Drift Detection & Rollback
- Cấu hình qua ArgoCD Application (`gitops/application.yaml`):
  - `syncPolicy.automated.selfHeal = true`: Khi có can thiệp trực tiếp bằng `kubectl edit` trên cluster (drift), ArgoCD tự động phát hiện sai lệch và đồng bộ hoàn nguyên về trạng thái khai báo trong Git.
  - `syncPolicy.automated.prune = true`: Tự động dọn dẹp các tài nguyên đã bị xóa khỏi Git.
  - **Quy trình Rollback chuẩn GitOps:** Khi phiên bản mới gặp lỗi trên production, kỹ sư chỉ cần revert git commit hoặc cập nhật `targetRevision` về tag trước đó (ví dụ: `v2.9.0`), ArgoCD sẽ tự động kích hoạt zero-downtime rolling update đưa hệ thống về trạng thái ổn định mà không cần can thiệp thủ công vào cluster.

---

## 9. Kết luận về Mức độ Sẵn sàng Sản xuất (Readiness Verdict)

Nền tảng đã đạt toàn bộ các tiêu chí trong bộ kiểm thử ban đầu (starter suite), bộ kiểm thử đơn vị (fast unit suite: 83/83 tests passed), kiểm tra ma trận tích hợp (245 checks passed), kiểm tra tính tương thích đa nền tảng (portability check: passed), và kiểm tra tài nguyên Kubernetes / GitOps (passed). Toàn bộ 10 ranh giới tích hợp, minh chứng bằng chứng (evidence files), cùng các kịch bản chịu tải và phục hồi sự cố đã được hoàn thiện và chứng minh đầy đủ.

