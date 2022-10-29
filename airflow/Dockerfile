# First-time build can take upto 10 mins.

FROM apache/airflow

SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

#COPY requirements.txt .
#RUN pip install --no-cache-dir -r requirements.txt

USER root
RUN apt-get update -qq && apt-get install vim procps -qqq
# git gcc g++ -qqq

ARG AIRFLOW_USER
ARG INSTALL_DIR
ARG JDK_FILE=openjdk-17.0.2_linux-x64_bin.tar.gz
ARG CLOUD_SDK_VERSION=407.0.0

ENV JAR_FILE_LOC=https://storage.googleapis.com/hadoop-lib/gcs/gcs-connector-hadoop3-latest.jar
ENV AIRFLOW_HOME=${INSTALL_DIR}/airflow
ENV GCLOUD_HOME=${INSTALL_DIR}/google-cloud-sdk
ENV JAVA_HOME=${INSTALL_DIR}/jdk-17.0.2

ENV PATH="${GCLOUD_HOME}/bin/:${JAVA_HOME}/bin:${PATH}"

RUN GCLOUD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && JDK_URL=https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/${JDK_FILE} \
    # install Java
    && wget "${JDK_URL}" --quiet \
    && tar --extract --gzip --file ${JDK_FILE} \
    && rm ${JDK_FILE} \
    # install gcloud SDK
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${GCLOUD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
    && mkdir -p "${GCLOUD_HOME}" \
    && tar xzf "${TMP_DIR}/google-cloud-sdk.tar.gz" -C "${GCLOUD_HOME}" --strip-components=1 \
    && "${GCLOUD_HOME}/install.sh" \
       --bash-completion=false \
       --path-update=false \
       --usage-reporting=false \
       # --additional-components alpha beta kubectl \
       --quiet \
    && rm -rf "${TMP_DIR}" \
    && rm -rf "${GCLOUD_HOME}/.install/.backup/" \
    && gcloud --version

USER ${AIRFLOW_USER}
