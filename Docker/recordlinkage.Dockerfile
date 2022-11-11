FROM openjdk:17.0.1
COPY /RecordLinkageInterface ./RecordLinkageInterface
WORKDIR /RecordLinkageInterface
RUN javac GreetServer.java
WORKDIR /
CMD [ "java", "RecordLinkageInterface.GreetServer" ]