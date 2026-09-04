# ANSWERS — Day 28 Track 2

Sinh viên: Lê Minh Đạt (2A202601088) — làm cá nhân, nhánh `ca-nhan-dat`.

> **Trạng thái bằng chứng.** Tài liệu này viết sau khi hoàn thành Bước 1–6. Bước
> 7–9 chưa chạy được trên máy cá nhân: README yêu cầu 12 GB trống cho hệ thống cơ
> bản và 20 GB cho toàn bộ hệ thống, máy chỉ có 4.4 GB, nên `lab28 preflight` xếp
> máy vào `profile: browser-fallback`. Mọi mục cần bằng chứng live được đánh dấu
> `UNVERIFIED` kèm lý do, đúng quy tắc "không giả lập" của rubric.

## 1. Bốn boundary đã hoàn thiện

Bốn hàm trong [`src/lab28_platform/integration_tasks.py`](src/lab28_platform/integration_tasks.py)
là phần code sinh viên sở hữu. Chúng không phải hàm tiện ích: Kafka producer,
Spark Delta MERGE, Feast client và handler `/ready` đều gọi thẳng vào đây, nên
một quyết định sai ở đây làm hỏng boundary tương ứng.

### `event_headers` — IP01 / IP10

Trả về `list[tuple[str, bytes]]`, và chỉ đính `traceparent` khi thực sự có trace.

Quyết định đáng nói là **bỏ hẳn header thay vì gửi chuỗi rỗng**. W3C Trace Context
quy định `traceparent` phải khớp định dạng `version-traceid-spanid-flags`; một
chuỗi rỗng là header hợp lệ về mặt Kafka nhưng vô nghĩa về mặt trace. Consumer khi
đó phải phân biệt "không có trace" với "có trace nhưng hỏng" — và cái sau thường
làm context propagation im lặng tạo ra một trace mồ côi. Không gửi header thì
consumer tự khởi tạo root span sạch sẽ.

Kiểu trả về là `list` chứ không phải `tuple` vì
[`event_bus.py:181`](src/lab28_platform/event_bus.py#L181) gọi `.append()` để
thêm `schema_version` vào kết quả. Đây là ràng buộc từ call site, không phải sở
thích.

### `dedupe_latest` — IP03

Giữ một event mới nhất cho mỗi `idempotency_key`, so sánh bằng tuple
`(occurred_at, event_id)`, và trả kết quả theo `sorted(keys)`.

Lý do kỹ thuật nằm ở Delta MERGE: nếu batch nguồn chứa hai dòng cùng khớp một
dòng đích, Delta ném lỗi thay vì tự chọn. Vậy nên batch **phải** unique trên
`idempotency_key` *trước khi* chạm writer. Đặt luật ở tầng Python thay vì trong
Spark job cho phép kiểm thử nó mà không cần JVM.

Hai chi tiết chống bất định:

- **Tie-break bằng `event_id`.** Hai event trùng `occurred_at` là chuyện bình
  thường khi producer bắn liên tiếp. Nếu chỉ so `occurred_at`, kết quả phụ thuộc
  thứ tự Kafka giao hàng — mà replay thì thứ tự đó khác lần chạy đầu. Đây đúng là
  thứ IT-J2-idempotent-replay đi tìm.
- **Sắp xếp theo key.** Output deterministic giúp Delta history diff đọc được, và
  làm bằng chứng time travel của IP03 so sánh được giữa các lần chạy.

### `feast_online_request` — IP04

Đọc `FEATURE_REFS` từ [`contracts.py:400`](src/lab28_platform/contracts.py#L400)
thay vì lặp lại bốn chuỗi feature.

Bốn tên feature này là contract giữa Delta, Feast registry và serving path. Viết
tay chúng ở đây tạo ra nguồn sự thật thứ hai, và nguồn thứ hai bao giờ cũng lệch
trước — thường vào lúc ai đó thêm feature vào registry mà quên sửa client. Import
constant khiến hai bên không thể lệch nhau mà vẫn qua được test.

`full_feature_names=False` giữ tên cột phẳng (`feedback_count`), khớp với cách
`_to_lookup` đọc response.

### `readiness_status` — IP07 / IP08

Phân ba trạng thái theo mức nghiêm trọng của probe: mandatory fail → `not_ready`
(thoát ngay), optional fail → `degraded`, còn lại → `ready`.

Đây là khác biệt giữa readiness và liveness mà rubric hỏi. `/health` trả lời
"tiến trình còn sống"; `/ready` trả lời "có nên gửi traffic tới không". Ba trạng
thái tách được ba tình huống vận hành khác nhau:

| Trạng thái | Ý nghĩa | Hành vi mong muốn |
|---|---|---|
| `ready` | Mọi dependency khoẻ | Nhận traffic bình thường |
| `degraded` | Dependency phụ hỏng | **Vẫn nhận traffic**, trả lời kèm cảnh báo |
| `not_ready` | Dependency bắt buộc hỏng | Rút khỏi load balancer |

Điểm quan trọng: `degraded` vẫn phục vụ. Feature store lạnh làm câu trả lời kém
chính xác hơn, nhưng vẫn là câu trả lời — trong khi vLLM chết thì không có gì để
trả về. Gộp hai ca này thành một trạng thái sẽ khiến Kubernetes rút pod khỏi
service chỉ vì Feast lag, tức tự gây outage từ một sự cố nhỏ.

Hàm nhận `Iterable` vì [`readiness.py:241`](src/lab28_platform/readiness.py#L241)
truyền vào một generator expression.

## 2. Trade-offs

**Dedupe ở Python thay vì trong Spark.** Đổi một chút hiệu năng (batch phải nằm
vừa bộ nhớ driver) lấy khả năng kiểm thử không cần JVM. Ở quy mô lab thì đúng; ở
production với batch hàng triệu dòng thì nên đẩy thành window function trong
Spark, phân vùng theo `idempotency_key` và sắp xếp giảm dần theo
`occurred_at, event_id`.

**`degraded` trả HTTP 200.** [`api.py:328`](src/lab28_platform/api.py#L328) chọn
200 kèm `degraded_reasons` trong body thay vì 503. Ưu điểm là client vẫn dùng
được câu trả lời; nhược điểm là monitoring dựa trên status code sẽ không thấy sự
cố — phải theo dõi metric riêng. Đây là lý do golden signal "errors" ở lab này
không thể chỉ đếm 5xx.

**Idempotency key do client cấp.** Cho phép replay an toàn, nhưng đẩy trách nhiệm
sinh key sang client. Client sinh key trùng cho hai nội dung khác nhau sẽ làm mất
dữ liệu một cách im lặng — Delta MERGE ghi đè mà không báo.

**Fail-fast trên probe bắt buộc.** `readiness_status` thoát ngay khi gặp
mandatory fail, nên response `/ready` không liệt kê đủ mọi probe hỏng. Đổi tính
đầy đủ của chẩn đoán lấy tốc độ trả lời — chấp nhận được vì `components` trong
body vẫn liệt kê đủ trạng thái từng thành phần.

## 3. Production gaps

Những chỗ hệ thống này chưa sẵn sàng cho production thật:

1. **Không có backpressure ở ingestion.** Producer `flush()` theo timeout; khi
   Kafka chậm kéo dài, API tích tụ request thay vì từ chối sớm. Cần bounded queue
   và trả 429 từ tầng ứng dụng, không chỉ ở Envoy.
2. **Rate limit chỉ ở gateway, và là local rate limit.** `envoy_http_local_rate_limit`
   đếm theo từng instance Envoy. Chạy nhiều replica gateway thì hạn mức thực tế
   nhân lên theo số replica. Production cần global rate limit service.
3. **PII redaction là regex.** [`guardrails.py`](src/lab28_platform/guardrails.py)
   thay thế theo mẫu, nên bắt được email/số điện thoại dạng chuẩn và bỏ sót dạng
   biến thể. Đủ cho lab, không đủ cho dữ liệu người dùng thật.
4. **Chỉ có hai alert.** [`monitoring/alerts.yml`](monitoring/alerts.yml) có
   `Lab28ApiUnavailable` và `Lab28HighErrorRatio`. Thiếu alert cho consumer lag,
   Feast freshness, và tỉ lệ `degraded` — tức ba chế độ hỏng đặc trưng nhất của
   kiến trúc này lại không có cảnh báo.
5. **Không có SLO burn-rate alert.** Alert hiện tại là ngưỡng tĩnh `for: 2m`.
   Production nên dùng multi-window burn rate để tránh vừa ồn vừa chậm.
6. **Champion alias không có canary.** IP06 promote bằng cách đổi alias, đổi là
   toàn bộ traffic chuyển ngay. Không có bước chia phần trăm traffic, nên rollback
   là cách duy nhất phát hiện model tệ — sau khi 100% người dùng đã gặp nó.
7. **Secret quản lý bằng biến môi trường.** `LANGSMITH_API_KEY` và credential
   vLLM đọc từ env. Production cần secret manager có xoay vòng và audit.
8. **Delta không có retention/vacuum policy.** Time travel giữ mọi version vô
   thời hạn, chi phí lưu trữ tăng tuyến tính.

## 4. Trạng thái 10 integration point

| IP | Boundary | Trạng thái |
|---|---|---|
| IP01 | HTTP → Kafka | Code xong (`event_headers`); evidence `UNVERIFIED` |
| IP02 | Kafka → Airflow | `UNVERIFIED` — cần profile `full` |
| IP03 | Airflow/Spark → Delta | Code xong (`dedupe_latest`); evidence `UNVERIFIED` |
| IP04 | Delta → Feast | Code xong (`feast_online_request`); evidence `UNVERIFIED` |
| IP05 | Delta → Qdrant | `UNVERIFIED` — cần stack |
| IP06 | Eval → MLflow Registry | `UNVERIFIED` — cần stack |
| IP07 | RAG → vLLM thật | `UNVERIFIED` — máy không GPU, chưa có endpoint lớp cấp |
| IP08 | Client → Envoy | Code xong (`readiness_status`); evidence `UNVERIFIED` |
| IP09 | → Prometheus/Grafana | `UNVERIFIED` — cần stack |
| IP10 | → OTLP trace | Code xong (`event_headers`); evidence `UNVERIFIED` |

Không có file nào trong `evidence/` được tạo. Rubric quy định làm giả bằng chứng
bị 0 điểm phần tương ứng, nên phần này để trống thay vì điền số phỏng đoán.

### Đã kiểm chứng được trên máy cá nhân

| Kiểm tra | Kết quả |
|---|---|
| `uv run pytest starter-tests -q` | 4 passed |
| `uv run pytest tests -q` | 83 passed |
| `uv run ruff check .` | All checks passed |
| `uv run python scripts/verify_matrix.py` | 245 checks passed |
| `uv run python scripts/check_portability.py` | OK |
| `uv run python scripts/validate_manifests.py` | passed |
| `docker compose --env-file ports.template config --quiet` | exit 0 |
| `docker compose --env-file ports.template --profile full config --quiet` | exit 0 |

## 5. Đóng góp

Làm cá nhân, nên toàn bộ năm vai trò trong matrix (`team-ingestion`, `team-data`,
`team-serving`, `team-platform`, `team-presenter`) do một người đảm nhiệm. Phần
đã hoàn thành trải trên bốn vai đầu: `event_headers` thuộc `team-ingestion`,
`dedupe_latest` và `feast_online_request` thuộc `team-data`, `readiness_status`
thuộc `team-serving` và `team-platform`.
