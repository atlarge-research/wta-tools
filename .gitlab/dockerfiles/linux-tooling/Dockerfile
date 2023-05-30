ARG base_image
FROM ${base_image}

RUN apt-get install -y sysstat
RUN apt-get install linux-tools-common && \
    apt-get install linux-tools-generic && \
    apt-get linux-tools-`uname -r`
