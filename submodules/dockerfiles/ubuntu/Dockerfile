ARG base_image
FROM ${base_image}

RUN apt-get install -y sysstat && \
    apt-get install -y dstat && \
    apt-get install -y linux-tools-common && \
    apt-get install -y linux-tools-generic && \
    apt-get install -y linux-tools-`uname -r`
