FROM openjdk:17.0.1
COPY /RecordLinkageInterface ./RecordLinkageInterface
WORKDIR /RecordLinkageInterface
RUN javac src/*.java
WORKDIR /RecordLinkageInterface/src
CMD [ "java", "Main.java", "" ]