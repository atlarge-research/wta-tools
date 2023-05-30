ARG base_image
FROM ${base_image}

RUN apt-get install -y sysstat && \
    sysctl kernel.perf_event_paranoid=-1
