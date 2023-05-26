FROM linux-tooling-setup-image

RUN wget https://dlcdn.apache.org/spark/spark-3.2.4/spark-3.2.4-bin-hadoop3.2.tgz
RUN tar xvf spark-3.2.4-bin-hadoop3.2.tgz
RUN curl -s "https://get.sdkman.io" | bash
RUN source "$HOME/.sdkman/bin/sdkman-init.sh"
RUN sdk install scala 2.13.5
RUN ls -la
