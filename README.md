# Usability enhancement for Kafka Streams testing - kafka-streams-test-topics

Kafka version 2.4.0 introduces TestInputTopic and TestOutputTopic classes with [KIP-470](https://cwiki.apache.org/confluence/display/KAFKA/KIP-470%3A+TopologyTestDriver+test+input+and+output+usability+improvements) 
to simplify the usage of the TopologyTestDriver interface. 
TestInputTopic class wraps TopologyTestDriver  and ConsumerRecordFactory methods as one class to be used to write to Input Topics 
and TestOutputTopic class collects TopologyTestDriver reading methods and provide typesafe read methods.

This project is class level compatible package for these classes, only different package name.
The kafka-streams-test-topics project has kafka-streams-test-utils as a compile time dependency only and
you need to include that as your own dependency to your project.
This way even the project is compiled using Kafka 2.3.0, 
you can use this also with any version of Kafka 2.0.0 and later.

# Usage

Add following dependency

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

## Usage in code

Replace existing TopologyTestDriver import  

    import org.apache.kafka.streams.TopologyTestDriver;
with the same name from new package 
com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver 
and import also other new classes.

    import com.github.jukkakarvanen.kafka.streams.test.TestInputTopic;
    import com.github.jukkakarvanen.kafka.streams.test.TestOutputTopic;
    import com.github.jukkakarvanen.kafka.streams.test.TestRecord;
    import com.github.jukkakarvanen.kafka.streams.test.TopologyTestDriver;

Follow the instruction in the tutorial in:
https://github.com/jukkakarvanen/kafka-streams-test-tutorial
The tutorial is compatible with this packages and same code can be found [examples](examples/) application forder.     

See also [JavaDoc](https://jukkakarvanen.github.io/kafka-streams-test-topics/)

## Maven repository info

* https://search.maven.org/artifact/com.github.jukkakarvanen/kafka-streams-test-topics/1.0.0/jar
* https://mvnrepository.com/artifact/com.github.jukkakarvanen/kafka-streams-test-topics        
        

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

## Testing Examples in Kafka Github:

* [WordCountDemoTest.java](https://github.com/apache/kafka/blob/trunk/streams/examples/src/test/java/org/apache/kafka/streams/examples/wordcount/WordCountDemoTest.java) 
* [TestTopicsTest.java](https://github.com/apache/kafka/blob/trunk/streams/test-utils/src/test/java/org/apache/kafka/streams/TestTopicsTest.java) 

## License
This project is licensed under [Apache License Version 2.0](LICENSE).
This might contain snippets of code from original Apache Kafka project, copyright the original author or authors.

## Author 

Questions and feedback jukka@jukinimi.com 