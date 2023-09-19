FROM eclipse-temurin:17-jre-focal

ADD https://downloads.apache.org/kafka/3.5.1/kafka_2.13-3.5.1.tgz kafka.tgz

RUN tar -xvzf kafka.tgz

RUN rm kafka.tgz

RUN mv /kafka_2.13-3.4.0 /kafka

ENV PATH=${PATH}:/kafka/bin

WORKDIR learning

ADD runner.sh runner.sh

CMD [ "sh", "runner.sh" ]