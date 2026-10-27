"""
실습 3-2. [정합성] 지각 이벤트 보내기 (이론 2.5 예시 그대로)

Event Time(이벤트가 실제로 발생한 시각)을 직접 정해서 메시지를 보냅니다.
  - 5분 Tumbling Window: 10:00~10:05
  - Grace Period 2분 → Stream Time이 10:07이 되면 10:00~10:05 창이 닫힘

보내는 순서:
  1) 10:01 주문   → 10:00~10:05 창
  2) 10:03 주문   → 10:00~10:05 창
  3) 10:06 주문   → Stream Time이 10:06으로 이동 (10:05~10:10 창)
  4) 10:01 주문   ← 지각! 하지만 Stream Time 10:06 < 10:07 이라 아직 받아줌
  5) 10:08 주문   → Stream Time이 10:08로 이동 → 10:00~10:05 창 닫힘
  6) 10:01 주문   ← 지각! Stream Time 10:08 ≥ 10:07 이라 버려짐

실행 (step3_2_window.py를 먼저 켜둔 상태에서, 다른 터미널에서):
  docker compose run --rm streams python step3_2_late_events.py
"""
import json
import time
from datetime import datetime, timezone

from confluent_kafka import Producer

from lab_common import BROKER

TOPIC = "window-demo"

# 실행할 때마다 다른 Key를 써서, 이전 실행의 Stream Time과 섞이지 않게 함
RUN_KEY = "coffee-" + datetime.now().strftime("%H%M%S")

SCENARIO = [
    ("10:01", ""),
    ("10:03", ""),
    ("10:06", "Stream Time → 10:06"),
    ("10:01", "지각! (Stream Time 10:06에 도착)"),
    ("10:08", "Stream Time → 10:08 (10:00~10:05 창 닫힘)"),
    ("10:01", "지각! (Stream Time 10:08에 도착)"),
]


def event_time_ms(hhmm: str) -> int:
    hour, minute = map(int, hhmm.split(":"))
    dt = datetime(2026, 1, 1, hour, minute, tzinfo=timezone.utc)
    return int(dt.timestamp() * 1000)


producer = Producer({"bootstrap.servers": BROKER})
print(f"'{TOPIC}' 토픽으로 이벤트를 보냅니다. (Key: {RUN_KEY})\n")

for i, (hhmm, note) in enumerate(SCENARIO, start=1):
    value = {"product_id": "coffee", "event_time": hhmm}
    # timestamp = Event Time. Window는 이 시각을 기준으로 바구니를 정함
    producer.produce(TOPIC, key=RUN_KEY, value=json.dumps(value), timestamp=event_time_ms(hhmm))
    producer.flush()
    print(f"{i}) 보냄: Event Time {hhmm}  {note}")
    time.sleep(2)

print("\n다 보냈어요. step3_2_window.py 터미널에서 결과를 확인하세요.")
