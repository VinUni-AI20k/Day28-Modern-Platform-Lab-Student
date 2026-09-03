# Lab 28 — Reflection và bàn giao

## Kiến trúc và ownership

Sơ đồ kiến trúc chính nằm tại [`docs/images/lab28-architecture-overview.svg`](docs/images/lab28-architecture-overview.svg). Luồng chính là Gateway → API → Kafka → Airflow/Spark → Delta Lake, sau đó materialize sang Feast và lập chỉ mục Qdrant; MLflow giữ release alias, còn Prometheus/Grafana và OpenTelemetry/Jaeger quan sát toàn bộ luồng.

| Phạm vi | Owner | Điểm kết nối |
|---|---|---|
| Ingestion | team-ingestion | IP01, IP02 |
| Data & ML | team-data | IP03, IP04, IP06 |
| Serving | team-serving | IP05, IP07 |
| Platform & observability | team-platform | IP08, IP09, IP10 |
| Demo/incident | team-presenter | evidence index, incident narrative, Q&A |

## Trade-offs kỹ thuật

1. Kafka tách ingestion khỏi xử lý batch, tăng khả năng replay nhưng đổi lại cần idempotency key, commit offset sau Delta MERGE và DLQ cho lỗi vĩnh viễn.
2. Delta MERGE theo khóa idempotency cho phép chạy lại an toàn và time travel, nhưng lịch sử nhiều file nhỏ cần compaction/vacuum có kiểm soát khi lên production.
3. Feast cung cấp contract online ổn định và freshness rõ ràng; hệ thống chấp nhận degraded response khi feature store tùy chọn lỗi thay vì che giấu bằng giá trị mặc định.
4. Qdrant dùng point ID xác định để re-index thành update, tránh nhân bản. Chất lượng retrieval vẫn phụ thuộc embedding model, hybrid weights và chiến lược re-index khi đổi model.
5. MLflow alias `champion` tách promotion/rollback khỏi image ứng dụng. Release phải ghim prompt, embedding model, vLLM model và Delta version để tái lập được.
6. Readiness phân biệt `ready`, `degraded`, `not_ready`: dependency bắt buộc làm pod không sẵn sàng; dependency tùy chọn phải hiển thị degraded và owner. Việc probe đồng bộ mọi dependency giúp chẩn đoán rõ nhưng tạo latency cao.

## Kết quả và bằng chứng

- Fast suite: 87 passed; Ruff, portability, matrix 245 checks và manifest validation đều đạt.
- J1: 12 passed, 3 GPU-gated skipped; trace live có gateway, API, Kafka producer/consumer, Airflow và Spark/Delta.
- J2: 9 passed; replay không tạo bản ghi trùng.
- Full local integration: 56 passed, 16 deselected theo đúng biểu thức không-GPU/không-LangSmith.
- MLflow champion sau rollback là version 1, run `5f1fc25837454495be6d58a4a8c58871`.
- Sau failure/recovery: feedback Delta v18/28 rows, documents Delta v12/23 rows, Qdrant 23 points, Feast healthy.
- Load profile 200/200 HTTP 200: P50 5281.94 ms, P95 5408.37 ms, P99 5501.87 ms. Bottleneck là probe vLLM không khả dụng chờ timeout trong mỗi lần `/ready`.
- IP07 là **UNVERIFIED**: `/version`, `/v1/models` và `/metrics` đều timeout; evidence ghi `reachable=false`, không dùng mock vLLM.

## Production gaps và hướng cải thiện

1. Chưa có vLLM GPU endpoint thật; cần endpoint được cấp, TLS, auth, network policy, quota và autoscaling trước khi xác nhận IP07.
2. Readiness cần cache ngắn hạn, timeout nhỏ và circuit breaker; deep diagnostics nên tách khỏi pod readiness để tránh P95 khoảng 5,6 giây.
3. Kafka một broker, Airflow standalone/SQLite và local Docker volume chỉ phù hợp lab; production cần HA, backup, retention, disaster recovery và capacity planning.
4. Secret hiện được truyền qua environment/runtime file; production cần secret manager, rotation và audit access.
5. Cần SLO/alert routing thực tế, trace retention/sampling, dashboard theo tenant và kiểm thử tải cho `/ask`, không chỉ `/ready`.
6. GitOps cần registry bất biến theo digest, ký image/SBOM, policy admission, promotion giữa môi trường và diễn tập rollback định kỳ.
7. Delta cần object storage, catalog/ACL, schema-evolution policy, compaction và vacuum retention an toàn.
8. Kaggle/tunnel chỉ là extension thử nghiệm: session, quota, cold start và URL tạm không đáp ứng production reliability hay security.

## Đóng góp cá nhân

Nguyễn Duy Trọng thực hiện bài cá nhân: đọc tài liệu và contracts; hoàn thiện bốn integration task; sửa seed client để tôn trọng rate limit bằng retry có giới hạn; dựng base/full stack; xử lý xung đột cổng và proxy OpenTelemetry; chạy unit, journey, failure/recovery, rollback, observability và load tests; thu evidence và viết bàn giao. Không sửa test, không đưa secret/runtime database/cache/model weights vào Git.
