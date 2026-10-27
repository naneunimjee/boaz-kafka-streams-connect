"""
Step 2-1. Stateless 처리 (filter / apply)

이론 2.4: Stateless = 이전 데이터 없이 "지금 들어온 메시지 하나"만 보고 처리
- 입력: orders.raw (Debezium이 MySQL 변경을 보낸 토픽)
- 처리: 변경 후 상태(after)만 꺼내고 → 20,000원 이상 주문만 통과
- 출력: 화면에 출력 (+ orders.filtered 토픽에 저장)
"""
import os

from quixstreams import Application

app = Application(
    broker_address=os.getenv("KAFKA_BROKER", "kafka:29092"),
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

# 2) TODO: 20,000원 이상 주문만 통과시키기
#    힌트: sdf.filter(lambda order: ...) — order는 {"order_id":..., "amount":..., ...} 형태의 dict
sdf = sdf.filter(lambda order: True)  # ← True 자리를 고쳐보세요

# 3) 결과를 화면에 출력하고 새 토픽에 저장
sdf.print(metadata=True)
sdf.to_topic(orders_filtered)

if __name__ == "__main__":
    app.run()
