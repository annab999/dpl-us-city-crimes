FROM continuumio/anaconda3

# loudly fail on error
SHELL ["/bin/bash", "-o", "pipefail", "-e", "-u", "-x", "-c"]

# arg and env vars
ARG SPARK_VERSION=spark-3.3.0-bin-hadoop3
ENV JAVA_HOME=${INSTALL_LOC}/jdk-17.0.2
ENV SPARK_HOME=${INSTALL_LOC}/${SPARK_VERSION}
ENV FILES_HOME=${SPARK_FILES_LOC}

# update system
# RUN useradd --create-home --shell /bin/bash --groups sudo anna
RUN apt-get update --quiet --yes \
	&& apt-get --quiet --yes install sudo man vim \
	&& apt-get --quiet --yes clean

# switch to user
# USER anna
WORKDIR ${INSTALL_LOC}

# downloads
RUN JDK_URL="https://download.java.net/java/GA/jdk17.0.2/dfd4a8d0985749f896bed50d7138ee7f/8/GPL/openjdk-17.0.2_linux-x64_bin.tar.gz" \
	&& SPARK_URL="https://dlcdn.apache.org/spark-3.3.0/${SPARK_VERSION}.tgz" \
	&& wget "${JDK_URL}" "${SPARK_URL}" --quiet \
	&& tar --extract --gzip --file openjdk-17.0.2_linux-x64_bin.tar.gz \
	&& tar --extract --gzip --file "${SPARK_VERSION}.tgz" \
	&& rm openjdk-17.0.2_linux-x64_bin.tar.gz "${SPARK_VERSION}.tgz"

# PATH env vars edit
ENV PATH="${SPARK_HOME}/bin:${JAVA_HOME}/bin:${PATH}"
ENV PYTHONPATH="${SPARK_HOME}/python/lib/py4j-0.10.9.5-src.zip:${SPARK_HOME}/python:$PYTHONPATH"

# switch back to proj dir
RUN mkdir "${FILES_HOME}"
WORKDIR "${FILES_HOME}"

# set up anaconda
RUN source /opt/conda/bin/activate

CMD [ "jupyter", "notebook", "--ip=0.0.0.0", "--no-browser" ]
