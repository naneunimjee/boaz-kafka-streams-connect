"""
실습 3-2. [정합성] Window + Stream Time + Grace Period (이론 2.5)

- 10초 Tumbling Window로 주문 수를 셉니다. (00~10초, 10~20초, ...)
- Grace Period 3초: 창이 끝나도 3초 동안은 늦게 온 이벤트를 받아줍니다.
- 창이 닫혔는지는 "벽시계"가 아니라 Stream Time
  (지금까지 본 가장 늦은 Event Time)으로 판단합니다.

화면에 나오는 것:
  ⏰ 지각 이벤트 도착 → ✅ 포함됨 / ❌ 버려짐
  📦 창이 닫히면 그 창의 최종 주문 수

실행 순서:
  터미널 1: docker compose run --rm streams python solutions/step3_2_window.py
  터미널 2: docker compose run --rm streams python step3_2_late_events.py
"""
from datetime import datetime, timedelta, timezone

from quixstreams import Application
from quixstreams.dataframe.windows import Count

from lab_common import BROKER

KST = timezone(timedelta(hours=9))
LATE_SEC = 2  # 이 이상 늦게 온 이벤트를 "지각"으로 표시

# 10초 Tumbling Window + Grace Period 3초
WINDOW_SIZE = timedelta(seconds=10)
GRACE = timedelta(seconds=3)

app = Application(
    broker_address=BROKER,
    consumer_group="step3-2-window",
    auto_offset_reset="latest",  # 이 앱을 켠 뒤에 들어온 이벤트만 처리
)

events = app.topic("window-demo", key_deserializer="str", value_deserializer="json")
sdf = app.dataframe(events)

stats = {"late_in": 0, "late_dropped": 0, "stream_time": 0}


def hms(ms: int) -> str:
    return datetime.fromtimestamp(ms / 1000, tz=KST).strftime("%H:%M:%S")


def show_late_arrival(value, key, timestamp, headers):
    # Stream Time = 지금까지 본 가장 늦은 Event Time
    stats["stream_time"] = max(stats["stream_time"], timestamp)
    if value["delay_sec"] >= LATE_SEC:
        print(f"⏰ 지각 이벤트 도착: #{value['order_no']} Event Time {hms(timestamp)} ({value['delay_sec']}초 늦음)")


def before_update(aggregated, value, key, timestamp, headers):
    # 창에 들어가기 직전에 호출됨 → 지각 이벤트가 여기까지 오면 "포함"된 것
    if value["delay_sec"] >= LATE_SEC:
        stats["late_in"] += 1
        size_ms = int(WINDOW_SIZE.total_seconds() * 1000)
        window_end = timestamp - timestamp % size_ms + size_ms
        if window_end <= stats["stream_time"]:
            print("   └ ✅ 포함됨 (창은 이미 끝났지만 Grace 안이라 받아줌)")
        else:
            print("   └ ✅ 포함됨 (창이 아직 진행 중)")
    return False  # True를 반환하면 창을 강제로 닫아버리므로 항상 False


def on_late(value, key, timestamp_ms, late_by_ms, start, end, store_name, topic, partition, offset):
    # 이미 닫힌 창에 들어가려는 이벤트 → 버려짐
    stats["late_dropped"] += 1
    print(f"   └ ❌ 버려짐 ({hms(start)}~{hms(end)} 창은 이미 닫힘)")
    return False  # True면 Quix 기본 경고 로그도 같이 출력


def show_closed_window(result: dict) -> None:
    print(
        f"\n📦 창 확정 {hms(result['start'])}~{hms(result['end'])}: 주문 {result['order_count']}건"
        f"   (지금까지 지각 포함 {stats['late_in']}건 / 버려짐 {stats['late_dropped']}건)\n"
    )


sdf = sdf.update(show_late_arrival, metadata=True)

sdf = sdf.tumbling_window(
    duration_ms=WINDOW_SIZE,
    grace_ms=GRACE,
    before_update=before_update,
    on_late=on_late,
)
sdf = sdf.agg(order_count=Count()).final()  # final(): 창이 닫힐 때 한 번만 결과 출력

sdf = sdf.update(show_closed_window)

if __name__ == "__main__":
    app.run()
