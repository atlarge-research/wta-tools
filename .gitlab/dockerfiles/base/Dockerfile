FROM ubuntu:jammy-20230425

RUN apt-get -y update
RUN apt-get install -y curl && \
    apt-get install -y zip && \
    apt-get install -y wget
RUN curl -s "https://get.sdkman.io" | bash && \
    bash -c 'source "/root/.sdkman/bin/sdkman-init.sh"; sdk install java 11.0.12-open' && \
    mkdir -p /usr/local/openjdk/11.0.12 && \
    mv -t /root/.sdkman/candidates/java/11.0.12-open /usr/local/openjdk/11.0.12 && \
    rm -rf /root/.sdkman/candidates/java
ENV JAVA_HOME=/usr/local/openjdk/11.0.12
RUN wget https://dlcdn.apache.org/maven/maven-3/3.9.2/binaries/apache-maven-3.9.2-bin.tar.gz && \
    tar xzvf apache-maven-3.9.2-bin.tar.gz && \
    rm -rf apache-maven-3.9.2-bin.tar.gz && \
    mkdir -p /opt/maven/3.9.2 && \
    mv -t apache-maven-3.9.2 /opt/maven/3.9.2
ENV MAVEN_HOME=/opt/maven/3.9.2 \
    MAVEN_CONFIG=/root/.m2
ENV PATH=$PATH:$MAVEN_HOME/bin
RUN mvn -v
