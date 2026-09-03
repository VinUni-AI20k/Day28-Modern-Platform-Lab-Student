# ANSWERS - Day 28 Track 2

Làm cá nhân. Branch `ca-nhan-hanh`.

## 1. Bốn hàm đã hoàn thiện

Chỉ sửa `src/lab28_platform/integration_tasks.py`.

### `event_headers` - IP01 + IP10

Trả `list` chứ không `tuple`, vì `event_bus.py:180` gọi `.append()` để thêm `schema_version` ngay sau đó.

Kiểm tra `if traceparent` thay vì `is not None`. Lý do: producer truyền `current_traceparent() or event.traceparent`, nên khi không có trace đang chạy thì giá trị là chuỗi rỗng. Gửi `traceparent=b""` tạo header W3C sai định dạng, làm đứt trace ở ranh giới Kafka.

### `dedupe_latest` - IP03

Duyệt input đúng một vòng. `delta_store.py:206` truyền `Iterable`, duyệt lần hai sẽ rỗng.

So sánh tuple `(occurred_at, event_id)` chứ không chỉ `occurred_at`. Hai event cùng key cùng thời điểm mà chỉ so timestamp thì kết quả phụ thuộc event nào tới trước, tức phụ thuộc thứ tự partition Kafka.

Sắp xếp output theo `idempotency_key`. Dict giữ thứ tự chèn, tức thứ tự Kafka; sort mới cho batch tái lập được giữa hai lần replay.

Dedupe trước khi MERGE, không phải trong Spark job. Delta MERGE ném lỗi khi source có hai dòng khớp cùng một target row. Đặt luật ở đây cho phép kiểm thử không cần JVM.

### `feast_online_request` - IP04

Lấy danh sách feature từ `contracts.FEATURE_REFS`, không chép lại bốn chuỗi. Đổi feature view lên v2 thì chỉ sửa một chỗ.

Bọc `list()` vì `FEATURE_REFS` là tuple, còn payload đi qua JSON.

`full_feature_names: False` để Feast trả tên ngắn `avg_rating` thay vì `asker_activity_v1__avg_rating`, khớp cách `_to_lookup` đọc response.

### `readiness_status` - IP07 + IP08

Duyệt một vòng, không dùng `any()` hai lần. `readiness.py:241` truyền generator expression, lần duyệt thứ hai luôn rỗng.

Thứ tự ưu tiên: mandatory fail thì trả `not_ready` ngay; optional fail chỉ bật cờ; còn lại `ready`.

## 2. Trade-off

| Quyết định | Chọn | Đánh đổi |
|---|---|---|
| Dedupe ở Python thay vì Spark | Python | Batch phải vừa bộ nhớ một tiến trình. Đổi lại kiểm thử được không cần JVM, và luật dedupe nằm cùng chỗ với contract |
| Tie-break bằng `event_id` | Có | Thêm một phép so sánh. Đổi lại kết quả không phụ thuộc thứ tự Kafka |
| Sort output theo key | Có | O(n log n) thay vì O(n). Đổi lại batch tái lập được, so sánh trước sau replay dễ |
| Bỏ `traceparent` khi rỗng | Bỏ hẳn header | Consumer phải chịu được thiếu header. Đổi lại không có header W3C hỏng đi vào Kafka |
| `require_real=true` mặc định | Bật | Không có vLLM thì `not_ready`. Đổi lại mock không bao giờ qua được gate IP07 |
| Rate limit ở gateway, không ở app | Gateway | App không biết mình bị chặn. Đổi lại chặn trước khi tốn tài nguyên app |

## 3. Production gap

Chưa đạt production. Các điểm còn thiếu:

- **IP07 chưa có bằng chứng thật.** GPU trên máy là GeForce 940MX, driver 516.54 (CUDA tối đa 11.7), VRAM 2048 MiB. Image `vllm/vllm-openai:v0.28.0` yêu cầu CUDA 12.x. Thử chạy thật thì báo `unsatisfied condition: cuda>=12.4`; hạ xuống image CUDA 11.8 vẫn không qua. Qwen3-1.7B ở FP16 cần khoảng 3.4 GB trọng số, vượt 2 GiB VRAM. Báo `UNVERIFIED` theo SUBMISSION.md, không giả lập. Hệ quả: `POST /api/v1/ask` trả 503 `dependency_unavailable` kèm `trace_id`, đúng contract lỗi nhưng không có luồng serving để demo.
- **`push_batch_metrics` được định nghĩa nhưng không nơi nào gọi.** Chi tiết ở mục 5.
- **Kafka không có volume.** `docker compose down` làm mất topic và message. Production cần persistent volume.
- **Replication factor = 1.** Mất broker là mất dữ liệu.
- **Không có Kubernetes thật.** Manifest và Argo CD Application chỉ validate tĩnh. Drift và self-heal chưa chạy được.
- **Không có SLO và alert routing.** Có alert rule nhưng chưa nối kênh nhận.
- **Rate limit cố định 10 req/s cho mọi client.** Không phân biệt theo tenant hay theo route.
- **Secret quản lý bằng env.** Production cần secret manager và xoay vòng khóa.
- **Corpus 13 document, 12 feedback.** Đủ để kiểm thử tích hợp, không đủ đo chất lượng retrieval.

## 4. Kết quả kiểm thử

| Lệnh | Kết quả |
|---|---|
| `pytest starter-tests -q` | 4 passed |
| `pytest starter-tests tests -q` | 87 passed |
| `ruff check .` | pass |
| `scripts/verify_matrix.py` | 245 checks passed |
| `scripts/check_portability.py` | pass |
| `scripts/validate_manifests.py` | pass |
| `pytest integration-tests -m "not gpu and not langsmith" -q` | 55 passed, 1 failed, 16 deselected |
| J1 golden path chạy riêng | 12 passed, 3 skipped |
| J2 idempotent replay | 9 passed |

### Test đỏ duy nhất

`test_gateway_rate_limit.py::test_the_gateway_answers_its_own_health_route` - flaky. Không sửa test.

Test kiểm tra gọi `/healthz` qua gateway thì counter `/health` của app không tăng. Helper `_health_requests` khớp theo tiền tố `route="/health`, tức đếm cả `/health` lẫn `/healthz`. Container `api` tự poll `/health` mỗi 5 giây qua Docker healthcheck (`compose.yaml:247-249`). Probe rơi vào khe giữa hai lần đọc counter thì lệch đúng 1 đơn vị.

Chạy lại 5 lần trên stack yên tĩnh: 4 passed, 1 failed, cùng chữ ký `519.0 == 518.0`.

Kết luận: gateway thật sự không proxy `/healthz`. Muốn hết flake thì tách metric label `/healthz` khỏi `/health`, nhưng không sửa test theo quy tắc của lab.

### Lưu ý khi chạy suite

Không chạy hai tiến trình pytest song song trên cùng stack. J4 dừng container, J3 đổi alias champion, `stack.py:114` gọi thẳng `docker compose`. Chạy song song sinh lỗi giả: `no Delta table at .lab28/delta/feedback` và `timed out waiting for spans`.

## 5. Quan sát được và chưa quan sát được

Đối chiếu 19 metric khai trong `contracts/integration-matrix.yaml` với Prometheus đang chạy: 13 có, 6 thiếu.

| Metric thiếu | IP | Nguyên nhân |
|---|---|---|
| `lab28_llm_tokens_total` | IP07 | thiếu vLLM |
| `lab28_feature_freshness_seconds` | IP04 | `/ask` không chạy được nên không có lookup từ đường serving |
| `otelcol_exporter_send_failed_spans` | IP10 | counter chỉ sinh khi có lỗi gửi span. Không có lỗi nên không có metric. Đây là dấu hiệu tốt, không phải thiếu sót |
| `lab28_pipeline_batches_total` | IP02 | xem bên dưới |
| `lab28_consumer_lag` | IP02 | xem bên dưới |
| `lab28_delta_version` | IP03 | xem bên dưới |
| `lab28_delta_rows_written_total` | IP03 | xem bên dưới |

### Lỗi tìm được trong scaffold

Bốn metric IP02 và IP03 có được set thật: DAG gọi `metrics.PIPELINE_BATCHES` ở `airflow/dags/lab28_ingestion_pipeline.py:153`, `delta_store.py:287` gọi `DELTA_VERSION.set()`.

Nhưng chúng chạy trong tiến trình Airflow worker ngắn hạn, Prometheus không scrape kịp. Hàm `metrics.push_batch_metrics` sinh ra đúng để xử lý việc này, docstring ghi rõ *"Pushing is used only for batch jobs"*. Grep toàn repo cho thấy hàm đó không được gọi ở đâu cả. Pushgateway chạy và được Prometheus scrape (job `lab28-airflow-batch` up), nhưng chỉ chứa metric Go runtime.

`scripts/verify_matrix.py` không bắt được vì nó kiểm cấu trúc và tham chiếu chéo, không kiểm metric sống.

Nằm ngoài phạm vi vòng đầu (chỉ sửa `integration_tasks.py`) nên để nguyên, ghi lại làm production gap.

### Golden signal có dữ liệu

| Tín hiệu | Giá trị đo được |
|---|---|
| Rate | 2.17 req/s |
| Duration P95 | 8.26 ms |
| Errors | 0 |
| Saturation, Kafka lag | 0 ở cả hai consumer group |

Grafana có dashboard `Lab 28 Platform Overview`, datasource Prometheus hoạt động.

### Độ phủ trace

Một trace mang 11 span, đủ 6 span bắt buộc của nửa ingestion:

```
gateway.request -> api.ingest -> kafka.produce -> kafka.consume -> airflow.dag -> spark.delta_merge
```

Thiếu 5 span của nửa serving: `lab28.api.ask`, `lab28.feast.get_online_features`, `lab28.qdrant.query`, `lab28.mlflow.resolve_release`, `lab28.vllm.chat_completion`. Tất cả nằm sau `/ask`, mà `/ask` cần vLLM.

Các assertion phủ đầy đủ trong `test_trace_span_coverage.py` mang marker `gpu` nên đã skip đúng thiết kế, không phải bị bỏ qua.

## 6. Promotion và rollback

Chạy thật, không sửa mã:

| Bước | Kết quả |
|---|---|
| Champion trước | v2 |
| `uv run lab28 rollback` | `champion moved from v2 to v1`, exit 0 |
| Champion sau | v1 |

Rollback là đổi alias trong Model Registry. Mã nguồn không đổi, service không restart. Đây là lý do đóng gói release bằng alias thay vì hardcode version trong code.

`test_j3_promotion_rollback.py` chạy cùng luật này và pass.

## 7. Sự cố và khôi phục

Bản ghi đầy đủ ở `evidence/failure-recovery.json`.

Giả thuyết viết trước khi inject: Feast là dependency không bắt buộc, dừng Feast thì `/ready` chuyển `degraded` và vẫn trả HTTP 200, không mất dữ liệu.

| Mốc | Thời điểm UTC | Quan sát |
|---|---|---|
| Baseline | 09:20:06 | `/ready` 200, delta feedback v9/20 rows, documents v6/18 rows, qdrant 18 points |
| Inject | 09:20:30 | `docker compose stop feast` |
| Observe | 09:20:35 | `/ready` 200, status `degraded`, component `feast` ready=false, detail `unreachable: ConnectError` |
| Recover | 09:20:50 | `start feast` rồi `up -d --wait feast`, container `Healthy` |
| Verify | 09:21:20 | delta feedback v9/20 rows, documents v6/18 rows, qdrant 18 points |

Chứng minh không mất dữ liệu: version và row count Delta giống hệt trước sau, Qdrant giữ nguyên 18 points, Kafka consumer lag = 0 ở cả hai group.

Không dùng `down -v` nên state giữ nguyên.

### Phân biệt ba trạng thái

| Trạng thái | Nghĩa | Gateway |
|---|---|---|
| `ready` | mọi dependency bắt buộc dùng được | nhận traffic |
| `degraded` | bắt buộc OK, optional hỏng | vẫn nhận traffic |
| `not_ready` | ít nhất một dependency bắt buộc hỏng, HTTP 503 | rút pod khỏi rotation |

`/health` là liveness, không chạm dependency. `/ready` là readiness, có chạm.

Cùng platform cho hai kết quả khác nhau tùy cấu hình:

| Đo từ | `LAB28_VLLM_REQUIRE_REAL` | Kết quả |
|---|---|---|
| `curl :8000/ready` trong container | `false` | 200, `degraded` |
| `uv run lab28 ready` từ host | `true` là mặc định | `not_ready` |

Cả hai đều đúng theo cấu hình của mình. Mức độ suy giảm là quyết định cấu hình, không phải code.

## 8. Hiệu năng

Phần cứng: 8 CPU, 15.5 GiB RAM, Docker Desktop trên Windows 10. Target `/ready` qua gateway `localhost:8080`.

| Workers | 200 | 429 | P50 | P95 | P99 |
|---|---|---|---|---|---|
| 8 | 200 | 0 | 788 ms | 985 ms | 1501 ms |
| 16 | 14 | 186 | 8 ms | 1198 ms | 1548 ms |

Lưu ý khi đọc số: `run_profile.py:24` dùng `except Exception: status = 0`, mà `urllib.urlopen` ném `HTTPError` cho mọi mã 4xx và 5xx. Nên `0` trong output không phải mất kết nối mà gộp cả 429. Đo trực tiếp xác nhận: burst 20 request qua gateway cho 15 lần 200 và 5 lần 429.

Điểm nghẽn không nằm ở app. Trong lúc chạy tải, API container dùng 6.21% CPU và 331 MiB trên 15.5 GiB, Kafka consumer lag = 0.

Nghẽn là token bucket của Envoy: `max_tokens: 10`, `fill_interval: 1s`. Ở 8 workers, một request `/ready` mất khoảng 788 ms nên tốc độ thực khoảng 10 req/s, vừa đúng bucket. Ở 16 workers, 429 trả về trong 8 ms nên worker bắn lại ngay, tạo vòng lặp phản hồi đẩy tốc độ vượt xa bucket, 93% bị chặn.

P50 giảm khi tăng tải là dấu hiệu của rate limit, không phải hệ thống nhanh lên.

Không suy ra capacity production từ con số này.

## 9. GitOps

Bản ghi ở `evidence/gitops-rollback.json`.

`scripts/validate_manifests.py` exit 0.

Desired state là tag Git cố định `refs/tags/v3.0.0`, image `ghcr.io/vinuni-ai20k/day28-platform-api:3.0.0`. Tag cố định nên rollback là đổi con trỏ Git, không build lại. Dùng `:latest` thì không biết đang chạy bản nào và không rollback được.

`selfHeal: true` nên sửa trực tiếp trên cluster sẽ bị revert. `revisionHistoryLimit: 5` cho 5 điểm quay lại.

Probe tách đúng vai: `startupProbe` và `livenessProbe` dùng `/health`, `readinessProbe` dùng `/ready`. Container chạy `runAsNonRoot`, `allowPrivilegeEscalation: false`, `readOnlyRootFilesystem: true`.

Drift và self-heal ghi `UNVERIFIED` vì không có cluster Kubernetes và Argo CD trong môi trường lab này.

## 10. Đóng góp

Làm cá nhân, không chia vai. Dùng năm vai trong `docs/team-role-cards.md` làm danh sách tự kiểm:

| Vai | Phạm vi | Trạng thái |
|---|---|---|
| Ingestion & Orchestration | IP01-IP02 | xong |
| Data & ML | IP03-IP04-IP06 | xong |
| Serving & Retrieval | IP05-IP07 | IP05 xong, IP07 `UNVERIFIED` |
| Platform & Observability | IP08-IP10 | xong |
| Presenter | evidence, incident, Q&A | xong |

## 11. Tổng kết trạng thái

`lab28 integration` cho score 83, `ready: false`.

| IP | Trạng thái |
|---|---|
| IP01 | ready |
| IP02 | có evidence file, không probe được từ serving process |
| IP03 | ready |
| IP04 | ready |
| IP05 | ready |
| IP06 | ready |
| IP07 | not_ready, thiếu vLLM thật |
| IP08 | có evidence file |
| IP09 | có evidence file |
| IP10 | có evidence file |

14 file trong `evidence/`. `ready: false` chỉ vì IP07.

Champion hiện ở v1 sau khi chạy demo rollback ở mục 6. `integration-report.json` sinh trước lúc đó nên ghi v2.

### Đối chiếu demo checklist

| Mục | Trạng thái | Ghi chú |
|---|---|---|
| Sơ đồ và 10 điểm kết nối | đủ vật liệu | `docs/images/`, integration matrix |
| Run ID, trace ID, Delta version, MLflow version | một phần | có đủ 4 loại ID nhưng ở các run khác nhau; luồng nối cả bốn là `/ask`, cần vLLM |
| Kafka replay không tạo row trùng | đạt | J2 9/9 passed |
| Sự cố có dự đoán, quan sát, khôi phục, no-data-loss | đạt | mục 7 |
| Grafana metrics và Jaeger trace xuyên hệ thống | một phần | 13/19 metric, 6/11 span bắt buộc |
| MLflow promote rồi rollback không sửa mã | đạt | mục 6, chạy live |
| Giải thích ready, degraded, not_ready | đạt | mục 7 |
| Triển khai và rollback K8s/GitOps | một phần | manifest validate exit 0, drift `UNVERIFIED` |
| Giải thích lựa chọn kỹ thuật | đạt | mục 1 và 2 |
| Không secret, temp, database, cache, weights trong Git | đạt | 103 file tracked, quét sạch |

Ba mục một phần quy về hai nguyên nhân: thiếu vLLM (hai mục) và không có cluster Kubernetes với Argo CD (một mục). Cộng thêm lỗi `push_batch_metrics` không được gọi, ảnh hưởng 4 trong 6 metric thiếu.
