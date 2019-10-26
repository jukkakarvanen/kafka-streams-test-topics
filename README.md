# Usability enhancement for Kafka Streams testing - kafka-streams-test-topics

TopologyTestDriver is a good test class providing possibility to test Kafka stream logic.
This is a lot of faster than utilizing EmbeddedSingleNodeKafkaCluster
The Kafka stream application code is very compact and the test code is easily a lot of bigger code base
 than actual implementation of the application. That's why it would be good to get test code easily readable and 
 understandable that way longer term also maintainable.


In Kafka version 2.4.0 introduced with [KIP-470](https://cwiki.apache.org/confluence/display/KAFKA/KIP-470%3A+TopologyTestDriver+test+input+and+output+usability+improvements) 
TestInputTopic and TestOutputTopic classes to simplify the usage of the test interface.
When using TopologyTestDriver prior version 2.4.0 you needed to call ConsumerRecordFactory to produce ConsumerRecord passed
into pipeInput method to write to topic. 
Also when calling readOutput to consume from topic, you needed to provide correct Deserializers each time. ProducerRecord
returned by readOutput contained a lot of extra fields set by Kafka internally which made the validating the records
more complicated.

You easily end up writing helper methods in your test classes, but this can be avoided when adding generic input and output topic classes wrapping existing functionality.

TestInputTopic class wraps TopologyTestDriver  and ConsumerRecordFactory methods as one class to be used to write to Input Topics 
and TestOutputTopic class collects TopologyTestDriver reading methods and provide typesafe read methods.

This project is class level compatible package for these classes, only different package name.
The kafka-streams-test-topics project has kafka-streams-test-utils as compile time dependency only and
you need to include that as your own dependency to your project.
This way even the project is compiled using Kafka 2.3.0. You can use this also with any version of Kafka 2.0.0 and later.

# Documentation        

The instruction of different test scenarions can be found from [README](examples/README.md) of [examples](examples/) application forder.     

See also [JavaDoc](https://jukkakarvanen.github.io/kafka-streams-test-topics/)

## Maven repository info
https://mvnrepository.com/artifact/com.github.jukkakarvanen/kafka-streams-test-topics        

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
            <version>1.0.0</version>
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
* [SimpleStreamAppTest.java](examples/src/test/java/com/github/jukkakarvanen/kafka/streams/example/SimpleStreamAppTest.java)

## Sample use of different methods
* [TestInputTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/TestInputTopicTest.java)
* [TestOutputTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/TestOutputTopicTest.java)


## Example how to simplify code 
[New versions](https://github.com/jukkakarvanen/kafka-streams-examples/blob/InputOutputTopic/src/test/java/io/confluent/examples/streams/WordCountLambdaExampleTest.java)
based on Confluent example and
[difference with original](https://github.com/jukkakarvanen/kafka-streams-examples/compare/5.2.1-post...jukkakarvanen:InputOutputTopic).
.
## License
This project is licensed under [Apache License Version 2.0](LICENSE).
This might contain snippets of code from original Apache Kafka project, copyright the original author or authors.