# Backend DEPORTEData - imagen optimizada para builds rápidos y robustos en CI
# Versiones (NO cambiar sin sincronizar con workers del cluster):
#   Spark  3.5.5 · Hadoop 3 · Python 3.10 (del base) · JDK 17

FROM eclipse-temurin:17-jdk-jammy AS builder

# Paquetes de sistema
ENV DEBIAN_FRONTEND=noninteractive

RUN sed -i 's|http://archive.ubuntu.com/ubuntu|https://mirror.cloudflarecdn.com/ubuntu|g; \
            s|http://security.ubuntu.com/ubuntu|https://mirror.cloudflarecdn.com/ubuntu|g' \
        /etc/apt/sources.list && \
    echo 'Acquire::Retries "5";'           > /etc/apt/apt.conf.d/80-retries && \
    echo 'Acquire::ForceIPv4 "true";'     >> /etc/apt/apt.conf.d/80-retries && \
    echo 'Acquire::http::Timeout "30";'   >> /etc/apt/apt.conf.d/80-retries && \
    echo 'Acquire::https::Timeout "30";'  >> /etc/apt/apt.conf.d/80-retries

RUN set -eux; \
    for i in 1 2 3; do \
      apt-get update && break || { \
        echo ">>> apt-get update falló (intento $i/3), espero 15s..."; \
        sleep 15; \
      }; \
    done; \
    apt-get install -y --no-install-recommends \
      ca-certificates curl wget unzip git maven openssh-client; \
    rm -rf /var/lib/apt/lists/*; \
    update-alternatives --install "/usr/bin/python" "python" "$(which python3)" 1


# Dependencias Python
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt && \
    rm /tmp/requirements.txt


# Apache Spark 3.5.5
# Mirror rápido (dlcdn) con fallback al archivo histórico.
ENV SPARK_VERSION=3.5.5 \
    HADOOP_VERSION=3 \
    SPARK_HOME=/opt/spark \
    PYTHONHASHSEED=1

RUN set -eux; \
    SPARK_TGZ="spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz"; \
    PRIMARY="https://dlcdn.apache.org/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"; \
    FALLBACK="https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/${SPARK_TGZ}"; \
    (wget -q "${PRIMARY}" -O /tmp/spark.tgz || wget -q "${FALLBACK}" -O /tmp/spark.tgz); \
    mkdir -p /opt/spark; \
    tar -xf /tmp/spark.tgz -C /opt/spark --strip-components=1; \
    rm /tmp/spark.tgz


# JARs vía Maven (hadoop-aws + mysql-connector)
WORKDIR /tmp/maven-build
COPY pom.xml .
RUN mvn -B -q --no-transfer-progress \
        dependency:copy-dependencies \
        -DoutputDirectory=/opt/spark/jars


# Imagen final
FROM builder AS deportedata-backend

ENV PATH="${SPARK_HOME}/bin:${SPARK_HOME}/sbin:${PATH}"

WORKDIR /opt/deportedata-backend

COPY app/ ./app/
COPY spark-jobs/ /opt/spark-apps/
COPY start-backend.sh /start-backend.sh
RUN sed -i 's/\r$//g' /start-backend.sh && chmod +x /start-backend.sh

EXPOSE 8000 8001

CMD ["/bin/bash", "/start-backend.sh"]
