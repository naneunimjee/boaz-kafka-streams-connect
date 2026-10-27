"""
실습 3-2. [정합성] Window + Stream Time + Grace Period (이론 2.5)

- 5분 Tumbling Window로 주문 수를 셉니다.
- Grace Period 2분: 창이 끝나고(10:05) 2분(10:07)까지는 지각 이벤트를 받아줍니다.
- 창이 닫혔는지는 "벽시계"가 아니라 Stream Time(지금까지 본 가장 늦은 Event Time)으로 판단합니다.

실행 순서:
  터미널 1: docker compose run --rm streams python solutions/step3_2_window.py
  터미널 2: docker compose run --rm streams python step3_2_late_events.py
"""
from datetime import datetime, timedelta, timezone

from quixstreams import Application
from quixstreams.dataframe.windows import Count

from lab_common import BROKER

app = Application(
    broker_address=BROKER,
    consumer_group="step3-2-window",
    auto_offset_reset="latest",  # 이 앱을 켠 뒤에 들어온 이벤트만 처리
)

events = app.topic("window-demo", key_deserializer="str", value_deserializer="json")
sdf = app.dataframe(events)


def hhmm(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).strftime("%H:%M")


def show_arrival(value, key, timestamp, headers):
    print(f"\n도착: Event Time {hhmm(timestamp)}")


def on_late(value, key, timestamp_ms, late_by_ms, start, end, store_name, topic, partition, offset):
    print(f"  ❌ 버려짐: {hhmm(start)}~{hhmm(end)} 창은 이미 닫혔어요 (Stream Time이 마감 시각을 넘음)")
    return False  # True면 Quix 기본 경고 로그도 같이 출력


def show_window(result: dict) -> None:
    print(f"  ✅ {hhmm(result['start'])}~{hhmm(result['end'])} 창의 주문 수: {result['order_count']}")


sdf = sdf.update(show_arrival, metadata=True)

# 5분 Tumbling Window + Grace Period 2분
# TODO: 5분짜리 Tumbling Window, Grace Period 2분으로 설정하기
#   힌트: timedelta(minutes=...) 를 사용
sdf = sdf.tumbling_window(
    duration_ms=timedelta(minutes=1),  # ← 고쳐보세요
    grace_ms=timedelta(minutes=0),     # ← 고쳐보세요
    on_late=on_late,
)
sdf = sdf.agg(order_count=Count()).current()  # current(): 창이 갱신될 때마다 결과 출력

sdf = sdf.update(show_window)

if __name__ == "__main__":
    app.run()
