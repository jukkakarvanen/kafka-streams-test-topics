# Examples for Testing Kafka Streams Using TestInputTopic and TestOutputTopic

Example files for Testing Kafka Streams Using TestInputTopic and TestOutputTopic.
This is utilizing older Kafka, JUnit 4 and AssertJ.

## Simple Kafka Stream test
* [SimpleStreamAppTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/example/SimpleStreamAppTest.java).

## Tutorial Example Test

Tutorial in https://github.com/jukkakarvanen/kafka-streams-test-tutorial
is compatible with this packages. Only difference with class imports. 
The example code for Kafka older than 2.4.0 can be found [here](src/test/java/com/github/jukkakarvanen/kafka/streams/example/MappingStreamAppTest.java)
with related example [pom.xml](pom.xml).

## Compatibility Test

Tests to validate the functionality to be equal with 2.4.0 can be found 
[here](src/test/java/com/github/jukkakarvanen/kafka/streams/test/).

**NOTE:** The difference of the functionality is when reading from uninitilized topic with
**kafka-streams-test-topics** return exception with message *Empty topic* instead of *Uninitilized topic*. 
