# Báo Cáo Phân Tích Kỹ Thuật & Tự Đánh Giá

**Học viên thực hiện:** Cá nhân  
**Nhánh bài làm:** `ca-nhan-student`  
**Đề tài:** Lab 28 Track 2 — Platform Integration & Production Readiness  

---

## 1. Các Đánh Đổi Kỹ Thuật

### 1.1. Delta Lake: `MERGE` thay vì `APPEND-ONLY` (IP03)
- **Bối cảnh:** Kafka cung cấp ngữ nghĩa truyền tải ít nhất một lần, dẫn đến khả năng xuất hiện bản ghi trùng lặp khi có sự cố mạng hoặc consumer replay.
- **Đánh đổi:**
  - *Phương án Append:* Tốc độ ghi cực cao vì chỉ nối đuôi tệp Parquet. Tuy nhiên, bảng Lakehouse sẽ bị nhân đôi số lượng bản ghi khi replay, làm sai lệch toàn bộ số liệu báo cáo và feature aggregate của Feast.
  - *Phương án MERGE (Được chọn):* Chi phí tính toán cao hơn do Spark phải đọc file, so sánh khóa `idempotency_key` và ghi lại Parquet files mới. Đổi lại, hệ thống đạt được tính **Idempotency tuyệt đối**, bảo đảm đúng đắn dữ liệu cho luồng AI/ML.

### 1.2. Phân tách Feast Offline Snapshot và Online Store (IP04)
- **Bối cảnh:** Quá trình suy luận của RAG API đòi hỏi độ trễ cực thấp (< 50ms) để lấy profile người dùng (`asker_activity_v1`).
- **Đánh đổi:**
  - Không thể truy vấn trực tiếp vào Delta Lake trong lúc serving vì độ trễ quét tệp dạng cột quá lớn.
  - Tách thành 2 tầng: Delta Lake đóng vai trò lưu trữ lịch sử và tính toán theo lô; định kỳ nạp sang Online Store với định dạng Key-Value để phục vụ tra cứu nhanh theo `asker_id`.
  - *Đánh đổi chấp nhận:* Dữ liệu đặc trưng có độ trễ cập nhật nhất định trong giới hạn cho phép, đổi lại độ trễ phục vụ suy luận đạt chuẩn P99.

### 1.3. Cơ chế phân loại Probe: `ready` vs `degraded` vs `not_ready` (IP07 & IP08)
- **Bối cảnh:** Kiến trúc microservices gồm nhiều dịch vụ phụ thuộc với mức độ quan trọng khác nhau (Kafka, Delta, Feast, Qdrant, vLLM).
- **Đánh đổi:**
  - Nếu áp dụng kiểm tra nhị phân đơn giản, khi vLLM chưa sẵn sàng hoặc gặp lỗi tạm thời, toàn bộ hệ thống API sẽ bị Gateway ngắt kết nối (503), làm gián đoạn cả các tác vụ nhận dữ liệu không dùng LLM.
  - Cơ chế 3 trạng thái phân biệt rõ `mandatory=True` và `mandatory=False`: Cho phép hệ thống hoạt động ở chế độ suy thoái mềm dẻo, phục vụ các luồng tiếp nhận và tìm kiếm bình thường trong khi chờ cụm GPU phục hồi.

---

## 2. Khoảng Cách Đến Môi Trường Production Thực Tế

Mặc dù bài lab mô phỏng đầy đủ 10 điểm kết nối, một hệ thống Production quy mô lớn cần giải quyết thêm các bài toán sau:

1. **Điều phối hạ tầng:**
   - *Trong bài lab:* Sử dụng Docker Compose với Envoy cấu hình tĩnh.
   - *Trong Production:* Triển khai trên cụm Kubernetes, quản lý thông qua **Kubernetes Gateway API** kết hợp **Argo CD** để đồng bộ khai báo GitOps tự động nhằm tự phát hiện cấu hình trôi dạt và tự phục hồi.
2. **Bảo mật và Quản lý Định danh:**
   - *Trong bài lab:* Các cổng và biến môi trường cấu hình qua `ports.template`.
   - *Trong Production:* Phải tích hợp HashiCorp Vault hoặc AWS Secrets Manager, xác thực phân quyền mTLS giữa các microservices và bảo vệ Kafka bằng SASL/SCRAM hoặc SSL.
3. **Mở rộng quy mô linh hoạt cho GPU:**
   - *Trong bài lab:* Một endpoint vLLM đơn lẻ.
   - *Trong Production:* Cần cơ chế tự động co giãn theo hàng đợi KEDA dựa trên số lượng token/s hoặc độ dài hàng đợi, triển khai vLLM trên cụm Ray Cluster với kỹ thuật phân chia mô hình và quản lý chi phí bằng cơ chế kết hợp tài nguyên trả trước và linh hoạt.
4. **Lưu trữ Lakehouse đám mây:**
   - *Trong bài lab:* Lưu trữ Delta Lake trên ổ đĩa cục bộ.
   - *Trong Production:* Delta Lake chạy trên lưu trữ đám mây S3/GCS với cơ chế tối ưu nén tệp tự động và chính sách dọn dẹp lịch sử định kỳ.

---

## 3. Phân Công Vai Trò & Đóng Góp

*(Thực hiện độc lập theo lộ trình cá nhân bao quát toàn diện cả 5 vai trò):*

| Vai trò kỹ thuật | Trách nhiệm thực hiện | Trạng thái |
|---|---|:---:|
| **1. Ingestion & Orchestration** | Cài đặt `event_headers` (IP01/IP10), Kafka correlation, Airflow pipeline contract. | Hoàn thành |
| **2. Data & ML** | Cài đặt `dedupe_latest` (IP03), Delta MERGE idempotency, Feast online request (IP04), MLflow release contract. | Hoàn thành |
| **3. Serving & Retrieval** | Xử lý Qdrant vector retrieval contract (IP05), vLLM inference endpoint identity & contract (IP07). | Hoàn thành |
| **4. Platform & Observability** | Xử lý `readiness_status` (IP08), xác thực GitOps manifests, OTLP span contract (IP10). | Hoàn thành |
| **5. Presenter / Delivery** | Chuẩn bị kịch bản demo sự cố, tài liệu phân tích trade-offs và báo cáo nghiệm thu. | Hoàn thành |
