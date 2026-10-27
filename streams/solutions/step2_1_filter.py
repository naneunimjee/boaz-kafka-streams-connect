"""
Step 2-1. Stateless 처리 (filter / apply)

이론 2.4: Stateless = 이전 데이터 없이 "지금 들어온 메시지 하나"만 보고 처리
- 입력: orders.raw (Debezium이 MySQL 변경을 보낸 토픽)
- 처리: 변경 후 상태(after)만 꺼내고 → 20,000원 이상 주문만 통과
- 출력: 화면에 출력 (+ orders.filtered 토픽에 저장)
"""
from quixstreams import Application

from lab_common import BROKER, check_input_topic

check_input_topic("orders.raw")  # 토픽이 없으면 Connector 등록하라고 안내하고 종료

app = Application(
    broker_address=BROKER,
    consumer_group="step2-1-filter",
    auto_offset_reset="earliest",  # 처음 실행할 때 토픽의 맨 처음부터 읽기
)

orders_raw = app.topic("orders.raw", key_deserializer="json", value_deserializer="json")
orders_filtered = app.topic("orders.filtered", key_serializer="json", value_serializer="json")

sdf = app.dataframe(orders_raw)

# 1) Debezium 메시지 { before, after, op, ... } 에서 after(변경 후 행)만 꺼내기
#    삭제(DELETE) 이벤트는 after가 없으므로 제외
sdf = sdf.filter(lambda msg: msg is not None and msg.get("after") is not None)
sdf = sdf.apply(lambda msg: {**msg["after"], "op": msg["op"]})

# 2) 20,000원 이상 주문만 통과
sdf = sdf.filter(lambda order: order["amount"] >= 20000)

# 3) 결과를 화면에 출력하고 새 토픽에 저장
sdf.print(metadata=True)
sdf.to_topic(orders_filtered)

if __name__ == "__main__":
    app.run()
