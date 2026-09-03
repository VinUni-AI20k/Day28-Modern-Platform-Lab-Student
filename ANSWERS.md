# Báo Cáo Phân Tích Kỹ Thuật & Tự Đánh Giá (ANSWERS.md)

**Học viên thực hiện:** Cá nhân  
**Nhánh bài làm:** `ca-nhan-student`  
**Đề tài:** Lab 28 Track 2 — Platform Integration & Production Readiness  

---

## 1. Các Đánh Đổi Kỹ Thuật (Architecture & Engineering Trade-offs)

### 1.1. Delta Lake: `MERGE` thay vì `APPEND-ONLY` (IP03)
- **Bối cảnh:** Kafka cung cấp ngữ nghĩa truyền tải ít nhất một lần (*at-least-once*), dẫn đến khả năng xuất hiện bản ghi trùng lặp khi có sự cố mạng hoặc consumer replay.
- **Đánh đổi:**
  - *Phương án Append:* Tốc độ ghi (write throughput) cực cao vì chỉ nối đuôi tệp Parquet. Tuy nhiên, bảng Lakehouse sẽ bị nhân đôi số lượng bản ghi khi replay, làm sai lệch toàn bộ số liệu báo cáo và feature aggregate của Feast.
  - *Phương án MERGE (Được chọn):* Chi phí tính toán cao hơn do Spark phải đọc file, so sánh khóa `idempotency_key` và ghi lại Parquet files mới. Đổi lại, hệ thống đạt được tính **Idempotency tuyệt đối** (chống trùng lặp tất định), bảo đảm đúng đắn dữ liệu cho luồng AI/ML.

### 1.2. Phân tách Feast Offline Snapshot và Online Store (IP04)
- **Bối cảnh:** Quá trình suy luận của RAG API đòi hỏi độ trễ cực thấp (< 50ms) để lấy profile người dùng (`asker_activity_v1`).
- **Đánh đổi:**
  - Không thể truy vấn trực tiếp vào Delta Lake trong lúc serving vì độ trễ quét tệp dạng cột (columnar scan) quá lớn.
  - Tách thành 2 tầng: Delta Lake đóng vai trò Offline Store (lưu trữ lịch sử, tính toán batch); định kỳ materialize sang Online Store (Redis / SQLite) với định dạng Key-Value phục vụ lookup theo `asker_id`.
  - *Đánh đổi chấp nhận:* Dữ liệu đặc trưng có độ trễ cập nhật (*data freshness delay*) trong giới hạn cho phép, đổi lại độ trễ phục vụ suy luận đạt chuẩn P99.

### 1.3. Cơ chế phân loại Probe: `ready` vs `degraded` vs `not_ready` (IP07 & IP08)
- **Bối cảnh:** Kiến trúc microservices gồm nhiều dependency với mức độ quan trọng khác nhau (Kafka, Delta, Feast, Qdrant, vLLM).
- **Đánh đổi:**
  - Nếu áp dụng kiểm tra nhị phân đơn giản (Healthy / Unhealthy), khi vLLM chưa sẵn sàng hoặc gặp lỗi tạm thời, toàn bộ hệ thống API sẽ bị Gateway ngắt kết nối (503), làm gián đoạn cả các tác vụ ingest dữ liệu không dùng LLM.
  - Cơ chế 3 trạng thái phân biệt rõ `mandatory=True` và `mandatory=False`: Cho phép hệ thống hoạt động ở chế độ suy thoái (*Graceful Degradation*), phục vụ các luồng ingestion và retrieval bình thường trong khi chờ cụm GPU phục hồi.

---

## 2. Khoảng Cách Đến Môi Trường Production Thực Tế (Production Gaps)

Mặc dù bài lab mô phỏng đầy đủ 10 điểm kết nối, một hệ thống Production quy mô lớn cần giải quyết thêm các bài toán sau:

1. **Điều phối hạ tầng (Orchestration & Gateway API):**
   - *Trong bài lab:* Sử dụng Docker Compose với Envoy cấu hình tĩnh.
   - *Trong Production:* Triển khai trên cụm Kubernetes (EKS/GKE), quản lý thông qua **Kubernetes Gateway API** (Envoy Gateway) kết hợp **Argo CD** để đồng bộ khai báo GitOps tự động (tự phát hiện cấu hình trôi dạt - *drift detection* và tự phục hồi - *self-healing*).
2. **Bảo mật và Quản lý Định danh (Security & Secret Management):**
   - *Trong bài lab:* Các cổng và biến môi trường cấu hình qua `ports.template`.
   - *Trong Production:* Phải tích hợp HashiCorp Vault hoặc AWS Secrets Manager, xác thực phân quyền mTLS giữa các microservices (Service Mesh - Istio/Linkerd) và bảo vệ Kafka bằng SASL/SCRAM hoặc SSL.
3. **Mở rộng quy mô linh hoạt cho GPU (GPU Auto-scaling & Quota):**
   - *Trong bài lab:* Một endpoint vLLM đơn lẻ.
   - *Trong Production:* Cần cơ chế tự động co giãn theo hàng đợi (KEDA) dựa trên số lượng token/s hoặc độ dài hàng đợi request, triển khai vLLM trên cụm Ray Cluster với Tensor Parallelism và quản lý chi phí bằng cơ chế kết hợp GPU On-demand và Spot instances.
4. **Lưu trữ Lakehouse đám mây (Cloud Object Storage):**
   - *Trong bài lab:* Lưu trữ Delta Lake trên volume cục bộ.
   - *Trong Production:* Delta Lake chạy trên S3/GCS/ADLS với cơ chế tối ưu nén tệp tự động (Auto-Compaction, Z-Ordering) và chính sách dọn dẹp lịch sử (VACUUM retention).

---

## 3. Phân Công Vai Trò & Đóng Góp

*(Thực hiện độc lập theo lộ trình cá nhân bao quát toàn diện cả 5 vai trò):*

| Vai trò kỹ thuật | Trách nhiệm thực hiện | Trạng thái |
|---|---|:---:|
| **1. Ingestion & Orchestration** | Cài đặt `event_headers` (IP01/IP10), Kafka correlation, Airflow pipeline contract. | Hoàn thành |
| **2. Data & ML** | Cài đặt `dedupe_latest` (IP03), Delta MERGE idempotency, Feast online request (IP04), MLflow release contract. | Hoàn thành |
| **3. Serving & Retrieval** | Xử lý Qdrant vector retrieval contract (IP05), vLLM inference endpoint identity & contract (IP07). | Hoàn thành |
| **4. Platform & Observability** | Xử lý `readiness_status` (IP08), xác thực GitOps manifests (Argo CD / K8s), OTLP span contract (IP10). | Hoàn thành |
| **5. Presenter / Delivery** | Chuẩn bị kịch bản demo sự cố, tài liệu phân tích trade-offs và báo cáo nghiệm thu. | Hoàn thành |
