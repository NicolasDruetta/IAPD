FROM python:3.15-slim-bookworm

ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1 \
    JAVA_HOME=/usr/lib/jvm/default-java \
    SPARK_HOME=/usr/local/lib/python3.15/site-packages/pyspark \
    PYSPARK_PYTHON=/usr/local/bin/python \
    PYSPARK_DRIVER_PYTHON=/usr/local/bin/python \
    PATH="/usr/local/lib/python3.15/site-packages/pyspark/bin:${PATH}"

WORKDIR /app

RUN apt-get update \
    && apt-get install --no-install-recommends -y \
        openjdk-17-jre-headless \
    && JAVA_BIN="$(readlink -f "$(command -v java)")" \
    && JAVA_DIR="$(dirname "$(dirname "$JAVA_BIN")")" \
    && test -x "$JAVA_DIR/bin/java" \
    && rm -rf /usr/lib/jvm/default-java \
    && ln -s "$JAVA_DIR" /usr/lib/jvm/default-java \
    && test -x "$JAVA_HOME/bin/java" \
    && "$JAVA_HOME/bin/java" -version \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt ./requirements.txt

RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -r requirements.txt

COPY main.py ./main.py
COPY pages ./pages
COPY assets ./assets
COPY README.md ./README.md
COPY AGENTS.md ./AGENTS.md

EXPOSE 8501 7077 8080 8081 8082

