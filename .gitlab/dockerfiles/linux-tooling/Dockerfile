ARG base_image
FROM ${base_image}

RUN lsb_release -r
RUN apt-get install -y sysstat && \
    apt-get install --reinstall -y linux-tools-common && \
    apt-get install --reinstall -y linux-tools-generic && \
    apt-get install --reinstall -y linux-tools-5.15.0-72-generic && \
    apt-get install --reinstall -y linux-tools-5.4.0-149-generic
