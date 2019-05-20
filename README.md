# Usability enhancement for Kafka Streams testing - kafka-streams-test-topics

The stream application code is very compact and the test code is a lot of bigger code base than actual implementation of the 
application, that's why it would be good to get test code easily readable and  understandable that way maintainable.

TopologyTestDriver is good test class providing possibility to test Stream logic without starting EmbeddedKafka instance.

When using TopologyTestDriver you need to call ConsumerRecordFactory to create ConsumerRecord passed into pipeInput method to write to topic. Also when calling readOutput to consume from topic, you need to provide correct Deserializers each time.

You easily end up writing helper methods in your test classes, but this can be avoided when adding generic input and output topic classes wrapping existing functionality.

TestInputTopic class wraps TopologyTestDriver  and ConsumerRecordFactory methods as one class to be used to write to Input Topics 
and TestOutputTopic class collects TopologyTestDriver reading methods and provide typesafe read methods.

These classes are proposed as improvements to main Apache Kafka project kafka-streams-test-utils package. 
 [KIP-456](https://cwiki.apache.org/confluence/display/KAFKA/KIP-456:+Helper+classes+to+make+it+simpler+to+write+test+logic+with+TopologyTestDriver) and
 [KAFKA-8233](https://issues.apache.org/jira/browse/KAFKA-8233)
 
 This project is class level compatible package for the planned classes, only different package name.
 The kafka-streams-test-topics project has kafka-streams-test-utils as compile time dependency only and
 you need to include that as your own dependency to your project.
 
 This way even the project is compiled using Kafka 2.2.0. You can use this also with any version of Kafka 1.1.0 and later.
 Only calls to pipeInput methods with Headers object (inputTopic.pipeInput(1L, "Hello", headers);) are failing, if used with with prior 2.0.0 version where header support was added.

There is a new [KIP-470](https://cwiki.apache.org/confluence/display/KAFKA/KIP-470%3A+TopologyTestDriver+test+input+and+output+usability+improvements) which contain the same classes
with little modifications. Migration from these classes also to KIP-470 should be rather straightforward operation. 


# Documentation        

See [JavaDoc](https://jukkakarvanen.github.io/kafka-streams-test-topics/)        

## Dependency in pom.xml


        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>com.github.jukkakarvanen</groupId>
            <artifactId>kafka-streams-test-topics</artifactId>
            <version>0.0.1-beta1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams-test-utils</artifactId>
            <version>${kafka.version}</version>
            <scope>test</scope>
        </dependency>
        

## Simple Stream Test Examples
* [SimpleTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/SimpleTopicTest.java)
* [SimpleStreamAppTest.java](examples/src/test/com/github/jukkakarvanen/kafka/streams/test/SimpleStreamAppTest.java)

## Sample use of different methods
* [TestInputTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/TestInputTopicTest.java)
* [TestOutputTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/TestOutputTopicTest.java)


## Example how to simplify code:
[New versions](https://github.com/jukkakarvanen/kafka-streams-examples/blob/InputOutputTopic/src/test/java/io/confluent/examples/streams/WordCountLambdaExampleTest.java)
based on Confluent example and
[Difference with original](https://github.com/jukkakarvanen/kafka-streams-examples/compare/TopologyTestDriver_tests...jukkakarvanen:InputOutputTopic#diff-eb92f3ffdd1c19905ffeba20a254eafc)
.
