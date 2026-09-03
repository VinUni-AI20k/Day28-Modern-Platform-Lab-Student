# Answers & Reflection — Day 28 Track 2

## Submission status

- Repository: `git@github.com:thanhvinh0702/Track2-Day28-HoangThanhVinh-2A202601124.git`
- Branch: update this line after pushing the personal/group branch.
- Code checks: 87 tests passed; Ruff, integration matrix, portability, and Kubernetes/GitOps manifest checks passed.
- Live evidence: not yet complete because the Docker image pull was interrupted before the stack started. Do not mark live IP evidence as passed until it is collected from the real services.

## Incident and recovery note

Incident used during local verification: the Compose stack could not finish starting because several large service images were still downloading and the progress stalled. Observed signs were no running containers in `docker compose ps --all`, missing Delta tables, unavailable Qdrant/Jaeger, and OTLP connection-refused messages. The immediate cause was incomplete service startup, not an application-code failure. Recovery is to rerun `docker compose up -d --build`, wait for all health checks, then run the core checkpoints and `uv run lab28 evidence` again. The current code-level checks show no data-loss regression; the live replay/no-data-loss proof still requires the integration journey.

## Reflection

### Khó nhất

Khó nhất là giữ đúng hợp đồng giữa các boundary: cùng một idempotency key phải được dùng từ Kafka đến Delta, trong khi traceparent phải được truyền dưới dạng header bytes và không được gửi rỗng.

### Trade-off

Khử trùng được thực hiện trước khi đưa dữ liệu vào Delta MERGE để tránh source có nhiều dòng cùng match một target row. Kết quả được sắp xếp theo key để tái lập, còn `FEATURE_REFS` được dùng trực tiếp từ contracts làm nguồn chuẩn duy nhất.

### Cải tiến tiếp theo

Hoàn thiện live evidence cho 10 integration points, chạy golden path/replay/recovery, thu P50/P95/P99, và bổ sung ảnh hoặc link Grafana/Jaeger cùng run ID, trace ID, Delta version và MLflow version.
