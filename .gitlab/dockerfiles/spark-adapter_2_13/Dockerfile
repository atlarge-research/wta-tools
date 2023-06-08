ARG base_image
FROM ${base_image}

RUN wget https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2-scala2.13.tgz && \
    tar xvzf spark-3.2.4-bin-hadoop3.2-scala2.13.tgz && \
    rm -rf spark-3.2.4-bin-hadoop3.2-scala2.13.tgz && \
    mkdir -p /opt/spark/3.2.4 && \
    mv spark-3.2.4-bin-hadoop3.2-scala2.13/* /opt/spark/3.2.4 && \
    rm -rf spark-3.2.4-bin-hadoop3.2-scala2.13
ENV SPARK_HOME=/opt/spark/3.2.4
RUN bash -c 'source "/root/.sdkman/bin/sdkman-init.sh"; sdk install scala 2.13.5' && \
    mkdir -p /usr/local/scala/2.13.5 && \
    mv /root/.sdkman/candidates/scala/2.13.5/* /usr/local/scala/2.13.5 && \
    rm -rf /root/.sdkman/candidates/scala
ENV SCALA_HOME=/usr/local/scala/2.13.5
ENV PATH=$PATH:$SPARK_HOME/bin:$SPARK_HOME/sbin
