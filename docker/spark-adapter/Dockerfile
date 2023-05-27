ARG base_image=maven:3-openjdk-11
FROM ${base_image}

RUN apt-get update
RUN wget https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
RUN tar xvf spark-3.2.4-bin-hadoop3.2.tgz
RUN rm -rf spark-3.2.4-bin-hadoop3.2.tgz
RUN apt-get install zip
RUN curl -s "https://get.sdkman.io" | bash
RUN bash -c 'source "/root/.sdkman/bin/sdkman-init.sh"; sdk install scala 2.13.5'
ENV SCALA_HOME=/root/.sdkman/candidates/scala
ENV SPARK_HOME=/spark-3.2.4-bin-hadoop3.2
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
