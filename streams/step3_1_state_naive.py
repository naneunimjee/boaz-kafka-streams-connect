"""
실습 3-1. [안정성] Stateful 처리를 "직접" 구현해보기 (라이브러리 없이 Consumer + Producer + dict)

이론 2.4: Stateful = 이전 데이터를 기억해야 하는 처리 (예: 상품별 주문 수 count)
- Kafka Streams 같은 라이브러리 없이, 일반 Consumer/Producer로 count를 흉내 냅니다.
- State Store 역할은 파이썬 dict(메모리)가 맡습니다.

실험: 몇 개 집계된 걸 확인한 뒤 Ctrl+C로 끄고 다시 켜보세요.
      → dict가 날아가서 count가 0부터 다시 시작합니다.
      → 그런데 Consumer는 "어디까지 읽었는지(offset)"를 기억하고 있어서
        끈 뒤의 메시지부터 읽습니다. 결과: 이전 주문이 빠진 "틀린" count!
"""
import json
from collections import defaultdict

from confluent_kafka import Consumer, Producer

from lab_common import BROKER, check_input_topic

INPUT_TOPIC = "orders.raw"
OUTPUT_TOPIC = "product-order-counts.naive"

check_input_topic(INPUT_TOPIC)

consumer = Consumer({
    "bootstrap.servers": BROKER,
    "group.id": "step3-1-state-naive",
    "auto.offset.reset": "earliest",  # 처음 실행할 때 토픽의 맨 처음부터 읽기
})
producer = Producer({"bootstrap.servers": BROKER})
consumer.subscribe([INPUT_TOPIC])

# ★ State Store 역할: 상품별 주문 수를 "메모리"에 저장 (프로그램 끄면 사라짐!)
state_store = defaultdict(int)

print("🧮 [직접 구현] 상품별 주문 수 집계 시작")
print("   State 저장 위치: 파이썬 dict (메모리) — 멈추기: Ctrl + C\n")
try:
    while True:
        msg = consumer.poll(1.0)
        if msg is None:
            continue
        if msg.error():
            print("에러:", msg.error())
            continue
        if msg.value() is None:  # 삭제 후 오는 빈 메시지(tombstone)는 건너뛰기
            continue

        event = json.loads(msg.value())
        # 새 주문만 센다: c = INSERT, r = Connector 시작 시 기존 행 스냅샷
        if event.get("op") not in ("c", "r"):
            continue
        product = event["after"]["product_id"]

        # ★ 핵심: 이전 count를 꺼내서 +1 하고 다시 저장
        state_store[product] += 1

        result = {"product_id": product, "order_count": state_store[product]}
        producer.produce(OUTPUT_TOPIC, key=product, value=json.dumps(result))
        producer.poll(0)

        print(f"🧮 직접 구현 │ {product:<9} +1 → 누적 {state_store[product]:>3}건")
except KeyboardInterrupt:
    print("\n🛑 멈췄어요. dict에 있던 count는 사라졌어요 → 다시 켜면 1건부터 다시 셉니다!")
finally:
    producer.flush()
    consumer.close()
