ARG base_image
FROM ${base_image}

RUN apt-get install -y sysstat && \
    apt-get install -y linux-perf && \
    apt-get install -y util-linux && \
    ln -sf /usr/bin/perf_5.10 /usr/bin/perf
