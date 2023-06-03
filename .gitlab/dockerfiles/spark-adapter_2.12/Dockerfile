ARG base_image
FROM ${base_image}

RUN wget https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz && \
    tar xvf spark-3.2.4-bin-hadoop3.2.tgz && \
    rm -rf spark-3.2.4-bin-hadoop3.2.tgz
RUN apt-get install zip && \
    curl -s "https://get.sdkman.io" | bash && \
    bash -c 'source "/root/.sdkman/bin/sdkman-init.sh"; sdk install scala 2.12.5'
ENV SCALA_HOME=/root/.sdkman/candidates/scala
ENV SPARK_HOME=/spark-3.2.4-bin-hadoop3.2
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
