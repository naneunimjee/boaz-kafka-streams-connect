"""
실습 3-1. [안정성] 같은 count를 Quix Streams로 (State Store + Changelog + Repartition 자동)

이론 2.2 / 2.4 / 2.4.1
- group_by("product_id"): Key를 order_id → product_id로 바꿔서 재분배 (Repartition)
  → Kafka UI에 "repartition__..." 토픽이 생깁니다.
- state.get / state.set: Key(상품)별로 값을 저장 (State Store, 디스크의 RocksDB)
  → Kafka UI에 "changelog__..." 토픽이 생깁니다. (State 백업 = Changelog)

실험 1: Ctrl+C로 끄고 다시 켜보세요. → count가 이어집니다! (step3_1_state_naive.py와 비교)
실험 2: 끈 상태에서 streams/state/step3-1-state-quix 폴더를 지우고 다시 켜보세요.
        → 로컬 State가 없어도 changelog 토픽에서 복구해서 count가 이어집니다.
"""
from quixstreams import Application, State

from lab_common import BROKER, check_input_topic

check_input_topic("orders.raw")

app = Application(
    broker_address=BROKER,
    consumer_group="step3-1-state-quix",
    auto_offset_reset="earliest",
)

orders_raw = app.topic("orders.raw", key_deserializer="json", value_deserializer="json")
counts_topic = app.topic("product-order-counts.quix", key_serializer="str", value_serializer="json")

sdf = app.dataframe(orders_raw)

# 1) 새 주문만 남기기 (c = INSERT, r = 스냅샷) + after만 꺼내기
sdf = sdf.filter(lambda msg: msg is not None and msg.get("op") in ("c", "r"))
sdf = sdf.apply(lambda msg: msg["after"])

# 2) Key를 상품 ID로 바꾸기 → Repartition
sdf = sdf.group_by("product_id")


# 3) 상품별 count (State는 지금 메시지의 Key = 상품별로 따로 저장됨)
def count_orders(order: dict, state: State) -> dict:
    # TODO: state에서 "order_count"를 꺼내서(없으면 0) +1 하고, 다시 state에 저장하기
    #   힌트: state.get("키", 기본값) / state.set("키", 값)
    count = 0  # ← 고쳐보세요
    return {"product_id": order["product_id"], "order_count": count}


sdf = sdf.apply(count_orders, stateful=True)

sdf.print()
sdf.to_topic(counts_topic)

if __name__ == "__main__":
    app.run()
