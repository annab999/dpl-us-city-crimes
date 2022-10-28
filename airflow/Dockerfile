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
ARG CLOUD_SDK_VERSION=406.0.0
ENV AIRFLOW_HOME=${INSTALL_DIR}/airflow
ENV GCLOUD_HOME=${INSTALL_DIR}/google-cloud-sdk
ENV JAVA_HOME=${INSTALL_DIR}/jdk-17.0.2

ENV PATH="${GCLOUD_HOME}/bin/:${PATH}"

RUN DOWNLOAD_URL="https://dl.google.com/dl/cloudsdk/channels/rapid/downloads/google-cloud-sdk-${CLOUD_SDK_VERSION}-linux-x86_64.tar.gz" \
    && JDK_URL=https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz \
    && wget "${JDK_URL}" "${SPARK_URL}" --quiet \
    && tar --extract --gzip --file openjdk-17.0.2_linux-x64_bin.tar.gz \
    && rm openjdk-17.0.2_linux-x64_bin.tar.gz \
    && TMP_DIR="$(mktemp -d)" \
    && curl -fL "${DOWNLOAD_URL}" --output "${TMP_DIR}/google-cloud-sdk.tar.gz" \
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

ENV PATH="${JAVA_HOME}/bin:${PATH}"
USER ${AIRFLOW_USER}
