# Báo Cáo Phân Tích Kỹ Thuật & Tự Đánh Giá

**Học viên thực hiện:** Đỗ Trung Kiên (Cá nhân) - 2A202601287 
**Nhánh bài làm:** `ca-nhan-kien`  
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

---

## 4. Số Liệu Kiểm Thử & Định Danh Hệ Thống Thực Tế

- **Định danh phiên bản mô hình MLflow:**
  - Tên mô hình: `lab28-rag-release`
  - Phiên bản: `v2` (gắn alias `champion`)
  - Mã lần chạy: `427170f1841c43ee8b486ecdf979882c`
- **Mã theo dõi phân tán:**
  - Trace ID ghi nhận qua Envoy Gateway & FastAPI: `40e1d98a70ecf222ec7e9b7c0046ac39`
- **Chỉ số kiểm thử tải:**
  - Tổng số yêu cầu kiểm thử: 100 requests (4 workers song song)
  - Tỷ lệ thành công: 96%
  - Độ trễ P50: 668.57 ms
  - Độ trễ P95: 4,376.00 ms
  - Độ trễ P99: 10,002.94 ms
- **Xác thực GitOps & Kubernetes:**
  - Script xác thực: `scripts/validate_manifests.py` đạt 100% hợp lệ.
  - Ma trận tích hợp: `scripts/verify_matrix.py` đạt 245/245 checks.

---

## 5. Ghi Chú Sự Cố Thực Nghiệm, Dấu Hiệu Quan Sát & Phục Hồi

- **Sự cố giả lập:** Ngắt kết nối dịch vụ suy luận mô hình vLLM trên cụm tính toán.
- **Dấu hiệu quan sát:**
  - Lệnh kiểm tra trạng thái ghi nhận probe `vllm` báo lỗi không thể kết nối.
  - Endpoint kiểm tra mức độ sẵn sàng tại cổng vào không bị sập và không trả về mã lỗi 503, mà tự động chuyển sang trạng thái suy thoái mềm dẻo `degraded`.
  - Các luồng tiếp nhận dữ liệu tài liệu qua Envoy Gateway và Kafka vẫn hoạt động bình thường, không bị gián đoạn.
  - Hệ thống giám sát Prometheus và Jaeger vẫn thu thập đầy đủ chỉ số và mã theo dõi.
- **Nguyên nhân gốc rễ:** Máy chủ suy luận vLLM chưa khởi chạy hoặc tài nguyên GPU chưa được kết nối.
- **Cách khôi phục & Chứng minh không mất dữ liệu:**
  - Kết nối lại endpoint vLLM từ máy chủ GPU hoặc hạ tầng dùng chung.
  - Sau khi kết nối lại, hệ thống tự động kiểm tra probe và chuyển trạng thái từ `degraded` sang `ready`.
  - Toàn bộ dữ liệu tài liệu và phản hồi đã gửi trong giai đoạn sự cố vẫn được lưu trữ an toàn trên hàng đợi Kafka và hoàn tất ghi nhận vào Lakehouse mà không bị thất thoát bất kỳ bản ghi nào.

---

## 6. Phần Suy Ngẫm & Đúc Kết (Reflection)

- **Điều khó nhất trong bài thực hành:**
  - Đảm bảo tính chống trùng lặp tuyệt đối khi dữ liệu được gửi lại nhiều lần qua Kafka. Việc thiết kế khóa định danh duy nhất và thuật toán so sánh cặp thời điểm xảy ra cùng mã sự kiện đòi hỏi sự chặt chẽ cao để không làm sai lệch bảng dữ liệu lớn.
  - Duy trì ngữ cảnh mã theo dõi phân tán W3C đi xuyên suốt qua nhiều ranh giới công nghệ khác nhau (Envoy Gateway, FastAPI, Kafka headers, cơ sở dữ liệu vector và hệ thống giám sát).
- **Các đánh đổi quan trọng đã lựa chọn:**
  - Chọn lệnh cập nhật có kiểm tra khóa thay vì chỉ nối đuôi dữ liệu để đổi lấy tính nhất quán tuyệt đối của dữ liệu.
  - Tách biệt kho lưu trữ dữ liệu lớn ngoại tuyến và kho lưu trữ đặc trưng trực tuyến tốc độ cao để đảm bảo độ trễ phục vụ người dùng dưới 50 mili-giây.
  - Áp dụng cơ chế ba trạng thái sẵn sàng để bảo vệ cổng giao tiếp không bị sập diện rộng khi một dịch vụ phụ gặp sự cố tạm thời.
- **Những điểm sẽ cải tiến khi triển khai thực tế:**
  - Đưa toàn bộ cấu hình khai báo lên cụm Kubernetes thật kết hợp công cụ GitOps Argo CD để tự động phát hiện sai lệch và tự phục hồi trạng thái mong muốn.
  - Xây dựng cơ chế tự động co giãn số lượng bản sao tính toán mô hình dựa trên độ dài hàng đợi yêu cầu thực tế.
  - Tích hợp hệ thống quản lý khóa bảo mật chuyên dụng để quản lý thông tin xác thực thay cho việc cấu hình qua biến môi trường.
