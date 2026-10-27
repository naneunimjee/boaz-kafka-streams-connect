"""
주문 데이터 자동 생성기 (Step 2부터 사용)

쇼핑몰 서버처럼 MySQL orders 테이블에 주문을 계속 만들고 상태를 바꿉니다.
  - 새 주문 생성:   INSERT (CREATED)
  - 결제:           UPDATE CREATED → PAID
  - 취소:           UPDATE CREATED/PAID → CANCELLED

MySQL이 바뀌면 Step 1에서 등록한 Debezium이 변경을 orders.raw 토픽으로 보냅니다.
(생성기는 Kafka를 전혀 모릅니다. DB만 건드려요!)

실행:
  docker compose run --rm streams python order_generator.py
  docker compose run --rm streams python order_generator.py --interval 0.5 --max-orders 20
멈추기: Ctrl + C
"""
import argparse
import os
import random
import time

import pymysql

# 상품 목록: (product_id, 가격)
PRODUCTS = [
    ("coffee", 4500),
    ("sandwich", 7000),
    ("book", 18000),
    ("mouse", 35000),
    ("keyboard", 89000),
    ("headset", 129000),
]
CUSTOMERS = [f"user{n:02d}" for n in range(1, 11)]  # user01 ~ user10


def connect():
    return pymysql.connect(
        host=os.getenv("MYSQL_HOST", "mysql"),
        port=int(os.getenv("MYSQL_PORT", "3306")),
        user=os.getenv("MYSQL_USER", "boaz"),
        password=os.getenv("MYSQL_PASSWORD", "boaz"),
        database=os.getenv("MYSQL_DATABASE", "shop"),
        autocommit=True,
    )


def next_order_id(cur):
    cur.execute("SELECT COALESCE(MAX(order_id), 0) FROM orders")
    return cur.fetchone()[0] + 1


def create_order(cur, order_id):
    product_id, price = random.choice(PRODUCTS)
    customer_id = random.choice(CUSTOMERS)
    quantity = random.randint(1, 3)
    amount = price * quantity
    cur.execute(
        "INSERT INTO orders (order_id, customer_id, product_id, amount, status) "
        "VALUES (%s, %s, %s, %s, 'CREATED')",
        (order_id, customer_id, product_id, amount),
    )
    print(f"🛒 주문   #{order_id:<4} {customer_id}  {product_id:<9} {amount:>9,}원")


def update_status(cur, order_id, status):
    cur.execute("UPDATE orders SET status=%s WHERE order_id=%s", (status, order_id))
    if status == "PAID":
        print(f"💳 결제   #{order_id:<4} → PAID")
    else:
        print(f"❌ 취소   #{order_id:<4} → CANCELLED")


def main():
    parser = argparse.ArgumentParser(description="주문 데이터 자동 생성기")
    parser.add_argument("--interval", type=float, default=1.0, help="한 번 움직이는 간격(초), 기본 1초")
    parser.add_argument("--max-orders", type=int, default=0, help="만들 주문 수 (0이면 Ctrl+C 할 때까지 계속)")
    parser.add_argument("--seed", type=int, default=None, help="같은 숫자를 주면 매번 같은 순서로 생성")
    args = parser.parse_args()

    if args.seed is not None:
        random.seed(args.seed)

    conn = connect()
    cur = conn.cursor()
    order_id = next_order_id(cur)
    created = 0
    open_orders = {}  # 아직 끝나지 않은 주문: {order_id: 현재 상태}

    print(f"🚀 주문 생성기 시작 (간격 {args.interval}초, 주문 번호 #{order_id}부터) — 멈추려면 Ctrl + C\n")
    try:
        while True:
            can_create = args.max_orders == 0 or created < args.max_orders
            if not can_create and not open_orders:
                print("\n✅ 지정한 주문을 모두 처리했어요. 종료합니다.")
                break

            # 열린 주문이 있으면 절반 확률로 그중 하나의 상태를 바꾸고, 아니면 새 주문 생성
            if open_orders and (not can_create or random.random() < 0.5):
                oid = random.choice(list(open_orders))
                state = open_orders[oid]
                if state == "CREATED":
                    # 결제 80%, 결제 전 취소 20%
                    new_state = "PAID" if random.random() < 0.8 else "CANCELLED"
                else:  # PAID
                    # 결제 후 취소 20%, 나머지는 구매 확정(더 이상 변경 없음)
                    new_state = "CANCELLED" if random.random() < 0.2 else None
                if new_state:
                    update_status(cur, oid, new_state)
                if new_state in (None, "CANCELLED"):
                    del open_orders[oid]
                else:
                    open_orders[oid] = new_state
            else:
                create_order(cur, order_id)
                open_orders[order_id] = "CREATED"
                order_id += 1
                created += 1

            time.sleep(args.interval)
    except KeyboardInterrupt:
        print("\n🛑 생성기를 멈췄어요.")
    finally:
        conn.close()


if __name__ == "__main__":
    main()
