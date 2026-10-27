-- MySQL 컨테이너가 처음 뜰 때 한 번만 실행됨
USE shop;

-- 운영 DB의 주문 테이블 (CDC 대상)
CREATE TABLE orders (
    order_id    INT PRIMARY KEY,
    customer_id VARCHAR(20) NOT NULL,
    product_id  VARCHAR(20) NOT NULL,
    amount      INT         NOT NULL,
    status      VARCHAR(20) NOT NULL,   -- CREATED / PAID / CANCELLED
    updated_at  TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP
);

-- Debezium이 binlog를 읽기 위한 전용 계정
CREATE USER 'debezium'@'%' IDENTIFIED BY 'dbz';
GRANT SELECT, RELOAD, SHOW DATABASES, REPLICATION SLAVE, REPLICATION CLIENT ON *.* TO 'debezium'@'%';
FLUSH PRIVILEGES;
