FROM python:3.9.15-bullseye

# loudly fail on error
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

USER root

# arg and env vars
ARG INSTALL_DIR
ARG MASTER_HOSTNAME
ARG JDK_FILE=openjdk-17.0.2_linux-x64_bin.tar.gz
ARG SPARK_VERSION=spark-3.3.1-bin-hadoop3
ARG JAR_FILE=gcs-connector-hadoop3-latest.jar
ARG JAR_LOC=${INSTALL_DIR}/hadoop-lib

ENV JAVA_HOME=${INSTALL_DIR}/jdk-17.0.2
ENV SPARK_HOME=${INSTALL_DIR}/${SPARK_VERSION}
ENV JAR_FILE_LOC=${JAR_LOC}/${JAR_FILE}
ENV SPARK_MASTER_HOSTNAME ${MASTER_HOSTNAME}

WORKDIR ${INSTALL_DIR}

# create user, update system
RUN useradd --shell /bin/bash --create-home --gid 0 spark-user \
    && mkdir -p ${JAR_LOC} \
    && apt-get update && apt-get -qq install vim \
    && apt-get autoremove -qq --purge && apt-get clean \
    # downloads
    && JDK_URL=https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/${JDK_FILE} \
    && SPARK_URL=https://dlcdn.apache.org/spark/spark-3.3.1/${SPARK_VERSION}.tgz \
    && CONNECTOR_URL=https://storage.googleapis.com/hadoop-lib/gcs/${JAR_FILE} \
    && wget "${JDK_URL}" "${SPARK_URL}" --quiet \
    && wget "${CONNECTOR_URL}" --directory-prefix ${JAR_LOC} --quiet \
    && tar --extract --gzip --file ${JDK_FILE} \
    && tar --extract --gzip --file ${SPARK_VERSION}.tgz \
    && rm ${JDK_FILE} ${SPARK_VERSION}.tgz \
    && chown -R spark-user "${SPARK_HOME}"

# PATH env vars edit
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${SPARK_HOME}/python:${PYTHONPATH}"

# set up work area
USER spark-user
COPY spark-env.sh "${SPARK_HOME}/conf/spark-env.sh"
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

CMD "${SPARK_HOME}/sbin/start-worker.sh" "spark://${SPARK_MASTER_HOSTNAME}:7077" \
        --cores 2 --memory 3G \
    && tail -f /dev/null
