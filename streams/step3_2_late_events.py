"""
실습 3-2. [정합성] 지각 이벤트가 섞인 주문 이벤트 생성기

실제 서비스에서는 이벤트가 "발생한 순서대로" 도착하지 않습니다.
(예: 지하철에서 폰이 잠깐 끊긴 상태로 주문 → 몇 초 뒤에 서버 도착)

이 생성기는 주문 이벤트를 계속 보내면서, 일부를 일부러 "늦게" 보냅니다.
  - 대부분: 지금 발생한 주문 (지연 0~1초)
  - 가끔(약 20%): 몇 초 전에 발생했는데 이제야 도착한 주문 (지연 2~12초)

Kafka 메시지의 timestamp = Event Time(주문이 실제로 발생한 시각)으로 넣습니다.
Window는 이 Event Time을 기준으로 바구니를 나눕니다.

※ 이 실습은 Event Time을 마음대로 조절하려고 MySQL/Debezium을 거치지 않고
   Kafka(window-demo 토픽)로 바로 보냅니다.

실행 (step3_2_window.py를 먼저 켜둔 상태에서, 다른 터미널에서):
  docker compose run --rm streams python step3_2_late_events.py
멈추기: Ctrl + C
"""
import argparse
import json
import random
import time
from datetime import datetime, timedelta, timezone

from confluent_kafka import Producer

from lab_common import BROKER

TOPIC = "window-demo"
KEY = "all-orders"  # 모든 주문을 하나의 Key로 → 전체 주문 수를 창별로 셈
PRODUCTS = ["coffee", "sandwich", "book", "mouse", "keyboard", "headset"]
KST = timezone(timedelta(hours=9))


def main():
    parser = argparse.ArgumentParser(description="지각 이벤트가 섞인 주문 이벤트 생성기")
    parser.add_argument("--interval", type=float, default=0.7, help="이벤트를 보내는 간격(초), 기본 0.7초")
    parser.add_argument("--late-ratio", type=float, default=0.2, help="지각 이벤트 비율, 기본 0.2 (20%%)")
    parser.add_argument("--seed", type=int, default=None, help="같은 숫자를 주면 매번 같은 패턴으로 생성")
    args = parser.parse_args()
    if args.seed is not None:
        random.seed(args.seed)

    producer = Producer({"bootstrap.servers": BROKER})
    print(f"'{TOPIC}' 토픽으로 주문 이벤트를 보냅니다. (멈추기: Ctrl + C)\n")

    n = 0
    try:
        while True:
            n += 1
            if random.random() < args.late_ratio:
                delay = random.uniform(2, 12)  # 지각 이벤트
            else:
                delay = random.uniform(0, 1)   # 정상 이벤트

            now_ms = int(time.time() * 1000)
            event_ms = now_ms - int(delay * 1000)  # 실제로 주문이 발생한 시각
            value = {"order_no": n, "product_id": random.choice(PRODUCTS), "delay_sec": round(delay, 1)}

            producer.produce(TOPIC, key=KEY, value=json.dumps(value), timestamp=event_ms)
            producer.poll(0)

            event_time = datetime.fromtimestamp(event_ms / 1000, tz=KST).strftime("%H:%M:%S")
            mark = f"  ← {delay:.0f}초 늦게 도착" if delay >= 2 else ""
            print(f"보냄 #{n:<4} Event Time {event_time}  {value['product_id']}{mark}")
            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n생성기를 멈췄어요.")
    finally:
        producer.flush()


if __name__ == "__main__":
    main()
