ARG base_image
FROM ${base_image}

RUN     wget https://dlcdn.apache.org/hadoop/common/hadoop-3.3.5/hadoop-3.3.5.tar.gz && \
        tar xvzf hadoop-3.3.5.tar.gz && \
        rm -rf hadoop-3.3.5.tar.gz && \
        mkdir -p /opt/hadoop/3.3.5 && \
        mv hadoop-3.3.5/* /opt/hadoop/3.3.5 && \
        rm -rf hadoop-3.3.5 \
ENV     HADOOP_HOME=/opt/hadoop/3.3.5
COPY    core-site.xml $HADOOP_HOME/etc/hadoop
ENV     PATH=$PATH:$HADOOP_HOME/bin
