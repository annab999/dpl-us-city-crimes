FROM continuumio/anaconda3

# loudly fail on error
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# arg and env vars
ARG INSTALL_DIR
ARG FILES_DIR
ARG SPARK_VERSION=spark-3.3.0-bin-hadoop3
ARG JAR_FILE=gcs-connector-hadoop3-latest.jar
ARG JAR_LOC=${INSTALL_DIR}/hadoop-lib

ENV JAVA_HOME=${INSTALL_DIR}/jdk-17.0.2
ENV SPARK_HOME=${INSTALL_DIR}/${SPARK_VERSION}
ENV FILES_HOME=${FILES_DIR}
ENV JAR_FILE_LOC=${JAR_LOC}/${JAR_FILE}

# update system, activate anaconda
# RUN useradd --create-home --shell /bin/bash --groups sudo anna
RUN apt-get update --quiet --yes \
    && apt-get --quiet --yes install sudo man vim \
    && apt-get --quiet --yes clean \
    && mkdir -p "${JAR_LOC}" "${FILES_HOME}" \
    && source /opt/conda/bin/activate

WORKDIR ${INSTALL_DIR}

# downloads
RUN JDK_URL=https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz \
    && SPARK_URL=https://dlcdn.apache.org/spark/spark-3.3.0/${SPARK_VERSION}.tgz \
    && CONNECTOR_URL=https://storage.googleapis.com/hadoop-lib/gcs/${JAR_FILE} \
    && wget "${JDK_URL}" "${SPARK_URL}" --quiet \
    && wget "${CONNECTOR_URL}" --directory-prefix ${JAR_LOC} --quiet \
    && tar --extract --gzip --file openjdk-17.0.2_linux-x64_bin.tar.gz \
    && tar --extract --gzip --file ${SPARK_VERSION}.tgz \
    && rm openjdk-17.0.2_linux-x64_bin.tar.gz ${SPARK_VERSION}.tgz

# PATH env vars edit
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${SPARK_HOME}/python:$PYTHONPATH"

# switch back to proj dir
WORKDIR "${FILES_HOME}"
#RUN pip install -r requirements.txt

CMD [ "jupyter", "notebook", "--ip=0.0.0.0", "--no-browser" ]
