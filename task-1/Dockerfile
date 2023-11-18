# Base image
FROM ubuntu:22.04

# Install dependencies
RUN apt -y update
RUN apt -y install maven openjdk-8-jdk wget

RUN wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.6/hadoop-3.3.6.tar.gz
RUN tar -xzvf hadoop-3.3.6.tar.gz
RUN mv hadoop-3.3.6 /opt/hadoop-3.3.6

# Environment variables
ENV JAVA_HOME=/usr/lib/jvm/java-11-openjdk-amd64

RUN mkdir /workdir 
WORKDIR /workdir

# Source code
# COPY prog /task
# VOLUME /task

# Build package
# WORKDIR /task
# RUN mvn -X clean package

# Run package
# ENTRYPOINT /opt/hadoop-3.3.6/bin/hadoop jar $JAR_FILE $JAVA_CLASS $TASK_ARGS
