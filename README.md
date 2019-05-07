# kafka-streams-test-topics
Kafka Streams test utility planned as improvement to Kafka kafka-streams-test-utils 
 [KIP-456](https://cwiki.apache.org/confluence/display/KAFKA/KIP-456:+Helper+classes+to+make+it+simpler+to+write+test+logic+with+TopologyTestDriver) and
 [KAFKA-8233](https://issues.apache.org/jira/browse/KAFKA-8233)
 
 This project is class level compatible package for the planned classes, only different package name.
 The kafka-streams-test-topics project has kafka-streams-test-utils as compile time dependency only and
 you need to include that as your own dependency to your project.
 
 This way even the project is compiled using Kafka 2.2.0. You can use this also with any version of Kafka 1.1.0 and later.
 Only calls to pipeInput methods with Headers object (inputTopic.pipeInput(1L, "Hello", headers);) are failing, if used with with prior 2.0.0 version where header support was added.

# Usage
in Pom.xml

        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams</artifactId>
            <version>${kafka.version}</version>
        </dependency>

        <!-- Test dependencies -->
        <dependency>
            <groupId>com.github.jukkakarvanen</groupId>
            <artifactId>kafka-streams-test-topics</artifactId>
            <version>0.0.1</version>
            <scope>test</scope>
        </dependency>
        <dependency>
            <groupId>org.apache.kafka</groupId>
            <artifactId>kafka-streams-test-utils</artifactId>
            <version>${kafka.version}</version>
            <scope>test</scope>
        </dependency>
        

## Simple Examples
* See [SimpleTopicTest.java](src/test/java/com/github/jukkakarvanen/kafka/streams/test/SimpleTopicTest.java)
* See [SimpleStreamAppTest.java](examples/src/test/com/github/jukkakarvanen/kafka/streams/test/SimpleStreamAppTest.java)

## Example how to simply code:
[New versions](https://github.com/jukkakarvanen/kafka-streams-examples/blob/InputOutputTopic/src/test/java/io/confluent/examples/streams/WordCountLambdaExampleTest.java)

[Difference with original](https://github.com/jukkakarvanen/kafka-streams-examples/compare/TopologyTestDriver_tests...jukkakarvanen:InputOutputTopic#diff-eb92f3ffdd1c19905ffeba20a254eafc)

