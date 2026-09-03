# Day 28 Track 2 — Báo cáo nộp bài cá nhân

## 1. Nhánh nộp bài

- Người thực hiện: Nguyễn Duy Trọng — 2A202601333
- Nhánh: `ca-nhan-nguyenduytrong`
- URL: <https://github.com/DuyTrongK64/Day28-Track2-2A202601333-NguyenDuyTrong/tree/ca-nhan-nguyenduytrong>

## 2. Kiến trúc, ownership và IP01–IP10

- [Sơ đồ kiến trúc](docs/images/lab28-architecture-overview.svg)
- [Integration matrix](contracts/integration-matrix.yaml)
- [Giải thích trade-off, production gaps và vai trò](ANSWERS.md)

| IP | Luồng | Owner | Bằng chứng |
|---|---|---|---|
| IP01 | Ingestion → Kafka | team-ingestion | [Kafka record và traceparent](evidence/ip01-kafka-consume.json) |
| IP02 | Kafka → Airflow | team-ingestion | [DAG run, task states, asset events](evidence/ip02-airflow-run.json) |
| IP03 | Airflow/Spark → Delta | team-data | [Delta history và time travel](evidence/ip03-delta-history.json) |
| IP04 | Delta → Feast | team-data | [Online feature, version và freshness](evidence/ip04-feast-online.json) |
| IP05 | Documents → Qdrant | team-serving | [Hybrid retrieval](evidence/ip05-qdrant-search.json) |
| IP06 | Release → MLflow | team-data | [Champion release](evidence/ip06-mlflow-release.json) |
| IP07 | Model → vLLM | team-serving | [Probe vLLM](evidence/ip07-vllm-identity.json) — **UNVERIFIED**, không có GPU endpoint |
| IP08 | Client → Envoy/API | team-platform | [200/429 và request ID](evidence/ip08-gateway.json) |
| IP09 | Services → Prometheus/Grafana | team-platform | [Targets](evidence/ip09-prometheus-targets.json), [dashboards](evidence/ip09-grafana-dashboards.json) |
| IP10 | Services → OpenTelemetry/Jaeger | team-platform | [Trace xuyên hệ thống](evidence/ip10-trace.json) |

## 3. Kết quả kiểm thử và integration matrix

- `uv run pytest tests -q`: **83 passed**.
- `uv run pytest starter-tests tests -q`: **87 passed**.
- J1 golden path: **12 passed, 3 GPU-gated skipped**.
- J2 replay: **9 passed**.
- `uv run pytest integration-tests -m "not gpu and not langsmith" -q`: **56 passed, 16 deselected**.
- `uv run ruff check .`: đạt.
- `uv run python scripts/verify_matrix.py`: **245 checks passed**.
- Portability và Kubernetes/GitOps manifest validation: đạt.
- [Kết quả fast suite](evidence/fast-suite.json)
- [Integration report](evidence/integration-report.json): 5/6 điểm probe trực tiếp đạt; IP07 chưa có vLLM thật.

## 4. Bằng chứng luồng đúng, replay-safe, metrics và trace

Happy path gần nhất có:

- Airflow run ID: `it-acaf8ec9`;
- trace ID: `285a4145bba64da2813feb65a69e736a` (khớp Kafka record và Airflow run);
- Delta version của online feature trong cùng J1: 13; trạng thái cuối là feedback v18/28 rows và documents v12/23 rows;
- MLflow champion version 1, run ID `5f1fc25837454495be6d58a4a8c58871`;
- J1 đã kiểm tra trace cùng ID chứa gateway, API, Kafka produce/consume, Airflow DAG và Spark Delta MERGE. Evidence IP10 bổ sung một trace continuity độc lập có ID `2ece8978d75645afafad254f5deba9ef`.

J2 gửi lại cùng idempotency key và chứng minh Delta chỉ giữ một row; Qdrant dùng point ID xác định nên re-index là update, không nhân bản. IP09 chứng minh target/rule/dashboard hoạt động; IP10 chứng minh cùng trace ID đi qua các hop bất đồng bộ.

## 5. Biên bản sự cố và khôi phục

- Dự đoán: khi dependency tùy chọn Feast/Qdrant bị dừng, tiến trình vẫn sống nhưng `/ready` và serving evidence phải chuyển sang `degraded`, nêu đúng component và owner.
- Tiêm lỗi: integration journey J4 dừng từng service thuộc phạm vi lab.
- Quan sát: readiness component về false; response có degraded reason; metrics/trace vẫn cho biết boundary lỗi.
- Nguyên nhân: dependency tùy chọn chủ động bị làm unavailable, không phải mất Kafka/Delta state.
- Khôi phục: service luôn được khởi động lại trong `finally`; probe được chờ đến khi healthy.
- Không mất dữ liệu: sau phục hồi Delta còn 28 feedback và 23 documents, Qdrant 23 points, Feast healthy và MLflow champion trở lại version 1.
- [Failure/recovery và no-data-loss proof](evidence/failure-recovery.json)

## 6. Load profile và reflection

Lệnh nộp chạy với `LAB28_GATEWAY_URL=http://127.0.0.1:18080` do cổng 8080 trên máy bị chiếm:

```text
uv run python load-tests/run_profile.py --requests 200 --workers 8
```

Kết quả: 200/200 HTTP 200; P50 5281.94 ms, P95 5408.37 ms, P99 5501.87 ms. Bottleneck là readiness probe gọi đồng bộ vLLM không tồn tại và chờ timeout khoảng 5 giây. Production nên cache kết quả probe ngắn hạn, dùng timeout/circuit breaker nhỏ hơn và tách lightweight readiness khỏi deep diagnostics. Chi tiết ở [load profile](evidence/load-profile.json).

Phần khó nhất là giữ trace context qua Kafka/Airflow/Spark và phân biệt lỗi ứng dụng với proxy do Docker Desktop chèn vào collector. Trade-off chính là ưu tiên evidence chẩn đoán đầy đủ trong readiness, đổi lại latency cao. Hướng cải thiện production được liệt kê trong [ANSWERS.md](ANSWERS.md).

## 7. Vai trò cá nhân và an toàn khi nộp

Đây là bài cá nhân. Nguyễn Duy Trọng đã đi qua đủ vai trò ingestion, data/ML, serving, platform/observability và presenter/incident commander. Repo nộp không chứa `.env`, token, mật khẩu, URL tạm có quyền truy cập, `.lab28`, database/cache Docker hoặc model weights.
