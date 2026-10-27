"""
실습 3-3. [확장성] Partition 기반 병렬 처리 (이론 2.2)

같은 앱을 여러 개 띄우면, 같은 consumer group끼리 파티션을 나눠 맡아서 병렬로 처리합니다.
  - 파티션 3개 + 앱 2개 → 한 앱이 2개, 다른 앱이 1개
  - 앱 하나를 끄면 → 남은 앱이 파티션을 전부 가져감 (Rebalance)
    이때 넘겨받은 파티션의 count는 changelog 토픽에서 복구해서 이어서 셉니다.

실행 (주문 생성기를 켜둔 상태에서, 터미널 2개):
  터미널 A: docker compose run --rm streams python step3_3_parallel.py A
  터미널 B: docker compose run --rm streams python step3_3_parallel.py B
"""
import sys

from quixstreams import Application, State, message_context

from lab_common import BROKER, check_input_topic

INSTANCE = sys.argv[1] if len(sys.argv) > 1 else "A"  # 앱 이름 (A, B, C, ...)

check_input_topic("orders.raw")

app = Application(
    broker_address=BROKER,
    consumer_group="step3-3-parallel",       # A, B 모두 같은 group → 파티션을 나눠 가짐
    auto_offset_reset="earliest",
    state_dir=f"state/step3-3-{INSTANCE}",   # 앱마다 State 폴더를 따로 써서 서로 충돌하지 않게
)

orders_raw = app.topic("orders.raw", key_deserializer="json", value_deserializer="json")
counts_topic = app.topic("product-order-counts.parallel", key_serializer="str", value_serializer="json")

sdf = app.dataframe(orders_raw)

# 1) 새 주문만 남기기 (c = INSERT, r = 스냅샷) + after만 꺼내기
sdf = sdf.filter(lambda msg: msg is not None and msg.get("op") in ("c", "r"))
sdf = sdf.apply(lambda msg: msg["after"])

# 2) Key를 상품 ID로 바꾸기 → Repartition (같은 상품은 항상 같은 파티션으로)
sdf = sdf.group_by("product_id")


# 3) 상품별 count + 지금 처리 중인 파티션 번호 기록
def count_orders(order: dict, state: State) -> dict:
    count = state.get("order_count", 0) + 1
    state.set("order_count", count)
    return {
        "product_id": order["product_id"],
        "order_count": count,
        "partition": message_context().partition,
    }


sdf = sdf.apply(count_orders, stateful=True)

sdf = sdf.update(
    lambda r: print(
        f"🔹 앱 {INSTANCE} │ 파티션 {r['partition']} │ {r['product_id']:<9} +1 → 누적 {r['order_count']:>3}건"
    )
)
sdf.to_topic(counts_topic)

if __name__ == "__main__":
    print(f"🔹 [앱 {INSTANCE}] 상품별 주문 수 집계 시작 (consumer group: step3-3-parallel)")
    print("   같은 group의 다른 앱과 파티션을 나눠 맡습니다 — 멈추기: Ctrl + C\n")
    app.run()
    print(f"\n🛑 앱 {INSTANCE}를 멈췄어요. 맡고 있던 파티션은 남은 앱이 넘겨받습니다!")
