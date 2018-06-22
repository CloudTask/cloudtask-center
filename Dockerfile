FROM frolvlad/alpine-glibc:alpine-3.6

MAINTAINER bobliu bobliu0909@gmail.com

RUN mkdir -p /opt/cloudtask/etc

RUN mkdir -p /opt/cloudtask/logs

RUN mkdir -p /opt/jobserver/etc/notify

COPY etc /opt/cloudtask/etc

COPY cloudtask-center /opt/cloudtask/cloudtask-center

COPY notify /opt/jobserver/etc/notify

WORKDIR /opt/cloudtask

VOLUME ["/opt/cloudtask/etc"]

VOLUME ["/opt/cloudtask/logs"]

CMD ["./cloudtask-center"]

EXPOSE 8985
