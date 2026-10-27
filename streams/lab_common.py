"""
실습 공통 유틸

check_input_topic: 입력 토픽이 Kafka에 있는지 먼저 확인합니다.
  Quix Streams는 토픽이 없으면 빈 토픽을 자동으로 만들어버려서,
  Connector를 등록 안 했을 때 "아무것도 안 나오는" 상태로 조용히 기다리게 됩니다.
  그걸 막기 위해 시작할 때 미리 확인하고 안내 메시지를 띄웁니다.
"""
import os
import sys

from confluent_kafka.admin import AdminClient

BROKER = os.getenv("KAFKA_BROKER", "kafka:29092")


def check_input_topic(name: str) -> None:
    topics = AdminClient({"bootstrap.servers": BROKER}).list_topics(timeout=10).topics
    if name not in topics:
        print(
            f"\n[실행 중단] '{name}' 토픽이 Kafka에 없어요.\n"
            "  → Step 1의 Source Connector(mysql-orders-source)를 먼저 등록하세요.\n"
            "  → 등록 상태 확인: curl http://localhost:8083/connectors/mysql-orders-source/status\n"
            "     (Windows PowerShell에서는 curl 대신 curl.exe)\n",
            file=sys.stderr,
        )
        sys.exit(1)
