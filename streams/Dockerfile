# 파이썬 Streams 앱 실행 환경 (모든 팀원이 같은 파이썬/라이브러리 버전을 쓰도록)
FROM python:3.12-slim

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# 실습 코드는 docker-compose에서 ./streams 폴더를 /app에 연결해서 사용
