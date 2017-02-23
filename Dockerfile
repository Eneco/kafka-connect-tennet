FROM eneco/connector-base:0.2.0

COPY build/libs/kafka-connect-tennet-0.2.0-all.jar /etc/kafka-connect/jars
