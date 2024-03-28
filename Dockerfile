# 使用官方的Python基础镜像
FROM python:3.9-slim-buster

EXPOSE 9000

WORKDIR /app

RUN apt-get update && apt-get install -y \
    build-essential \
    software-properties-common \
    && rm -rf /var/lib/apt/lists/*

COPY requirements /app
RUN pip3 install -r requirements.txt

COPY ./src /app/src

ENV TZ="Asia/Shanghai"

CMD cd src && uvicorn fc_test:api --host=0.0.0.0 --port=9000

