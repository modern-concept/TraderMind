FROM openjdk:11

# 🧰 Instalações básicas
RUN apt-get update && \
    apt-get install -y wget curl python3 python3-pip && \
    apt-get clean

# 🔥 Variáveis do Spark
ENV SPARK_VERSION=3.5.1
ENV HADOOP_VERSION=3

# 📦 Instalação do Spark
RUN wget --timeout=120 --tries=3 https://archive.apache.org/dist/spark/spark-${SPARK_VERSION}/spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    tar xvf spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz && \
    mv spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION} /opt/spark && \
    rm spark-${SPARK_VERSION}-bin-hadoop${HADOOP_VERSION}.tgz

ENV SPARK_HOME=/opt/spark
ENV PATH=$SPARK_HOME/bin:$PATH
ENV JAVA_HOME=/usr/local/openjdk-11
ENV PATH=$JAVA_HOME/bin:$PATH
ENV PYSPARK_PYTHON=python3
ENV PYSPARK_DRIVER_PYTHON=python3


# 🐍 Instalação de dependências Python com controle de timeout
COPY requirements.txt .
RUN pip install --default-timeout=100 --no-cache-dir -r requirements.txt
RUN pip install -r requirements.txt

# 🚧 Diretório de trabalho
WORKDIR /app

# 📁 Copia os scripts para o contêiner (ajuste o caminho conforme seu contexto de build)
COPY scripts /app/scripts

RUN python3 --version && java -version