FROM openjdk:17.0.1
COPY /RecordLinkage ./RecordLinkage
WORKDIR /RecordLinkage
RUN javac GreetServer.java
WORKDIR /
CMD [ "java", "RecordLinkage.GreetServer" ]