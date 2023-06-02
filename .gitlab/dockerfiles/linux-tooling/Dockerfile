ARG base_image
FROM ${base_image}

RUN apt-get install -y sysstat && \
    apt-get install -y linux-tools-common && \
    apt-get install -y linux-tools-generic && \
    apt-get install -y linux-tools-5.15.0-72-generic && \
    apt-get install -y linux-tools-5.4.0-149-generic
