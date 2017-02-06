FROM eneco/connector-base:0.2.0

COPY build/libs/kafka-connect-tennet-1.0.1-all.jar /etc/kafka-connect/jars
