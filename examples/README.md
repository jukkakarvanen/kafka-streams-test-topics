# Testing Kafka Streams Using TestInputTopic and TestOutputTopic

TopologyTestDriver is a good test class providing possibility to test Kafka stream logic.
This is a lot faster than utilizing EmbeddedSingleNodeKafkaCluster.
The Kafka stream application code is very compact and the test code is easily a lot bigger code base
 than the actual implementation of the application. That is why it would be good to get the test code to be easily readable and 
 understandable. This way the longer term is also more maintainable.


In Kafka version 2.4.0 introduced with [KIP-470](https://cwiki.apache.org/confluence/display/KAFKA/KIP-470%3A+TopologyTestDriver+test+input+and+output+usability+improvements) 
TestInputTopic and TestOutputTopic classes to simplify the usage of the test interface.
When using TopologyTestDriver prior version 2.4.0 the code needed to call ConsumerRecordFactory to produce ConsumerRecord passed
into pipeInput method to write to the topic. 
Also when calling readOutput to consume from the topic, the code needed to provide correct Deserializers each time. ProducerRecord
returned by readOutput contained a lot of extra fields set by Kafka internally which made validating the records
more complicated.


Additionally there exists a separate [Fluent Kafka Streams Tests](https://medium.com/bakdata/fluent-kafka-streams-tests-e641785171ec) 
wrapper around TopologyTestDriver, but I prefer using the same assertion framework as with all the other tests. Kafka Streams itself
is using [Hamcrest](http://hamcrest.org/JavaHamcrest/tutorial), but I like a [AssertJ](https://assertj.github.io/doc/)
with it's ease of use what auto-completion in IDE is offering. 
This page provides examples on how to write Kafka Streams test with AssertJ.  

## Setup

General information about testing can be found in the [Kafka Streams - Developer Guide](https://kafka.apache.org/documentation/streams/developer-guide/testing.html).
The first step in the test class is to create TopologyTestDriver and related TestInputTopic and 
TestOutputTopic. These TestInputTopic and TestOutputTopic are new since Kafka version 2.4.0. If the test does not need to validate 
the record time, it can create InputTopic without base time and advance information.

    private TopologyTestDriver testDriver;
    private TestInputTopic<Long, String> inputTopic;
    private TestOutputTopic<String, Long> outputTopic;

    private final Instant recordBaseTime = Instant.parse("2019-06-01T10:00:00Z");
    private final Duration advance1Min = Duration.ofMinutes(1);

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        MappingStreamApp.createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), MappingStreamApp.getStreamsConfig());
        inputTopic = testDriver.createInputTopic(MappingStreamApp.INPUT_TOPIC, new LongSerializer(), new StringSerializer(), 
                recordBaseTime, advance1Min);
        outputTopic = testDriver.createOutputTopic(MappingStreamApp.OUTPUT_TOPIC, new StringDeserializer(), new LongDeserializer());
    }

## Testing One Record at a Time
### Testing record value

In this example the stream that the code is testing, is swapping the key as a value and the value as a key. 
For that purpose the code is piping the key and the value, but there is a possibility to write only the value.
In this example the test is asserting only the value, so it uses readValue method for it. 

    @Test
    public void testOnlyValue() {
        //Feed 9 as key and word "Hello" as value to inputTopic
        inputTopic.pipeInput(9L, "Hello");
        //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
        assertThat(outputTopic.readValue()).isEqualTo(9L);
        //No more output in topic
        assertThat(outputTopic.isEmpty()).isTrue();
    }

At the end of the test isEmpty is assuring that there are no more messages in the topic. Earlier, before 2.4.0 version, the code 
read the record and the validated returned record was null. Now, if the test reads from empty topic, it is generating exception. 
That way the test can validate with readValue as well, if it is expecting to get the null value.
Additionally, now the reading from non-existing topic is causing exception instead of the null value, as it did before.

    @Test
    public void testReadFromEmptyTopic() {
        //Reading from empty topic generate Exception
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> { outputTopic.readValue(); })
                .withMessage("Empty topic: %s", MappingStreamApp.OUTPUT_TOPIC);
    }

### Testing KeyValue

If the test needs to validate both the key and the value, it can use readKeyValue and assert it against the KeyValue.

    @Test
    public void testKeyValue() {
        //Feed 9 as key and word "Hello" as value to inputTopic
        inputTopic.pipeInput(9L, "Hello");
        //Read KeyValue and validate it, timestamp is irrelevant in this case
        assertThat(outputTopic.readKeyValue()).isEqualTo(new KeyValue<>("Hello", 9L));
        //No more output in topic
        assertThat(outputTopic.isEmpty()).isTrue();
    }

### Testing with TestRecord

If the test also needs to validate record time, it can use readRecord, and assert it against TestRecord. The TestRecord
contructors support both Instant and the old way with long timestamps.

    @Test
    public void testKeyValueTimestamp() {
        final Instant recordTime = Instant.parse("2019-06-01T10:00:00Z");
        //Feed 9 as key and word "Hello" as value to inputTopic with record timestamp
        inputTopic.pipeInput(9L, "Hello", recordTime);
        //Read TestRecord and validate it
        assertThat(outputTopic.readRecord()).isEqualTo(new TestRecord<>("Hello", 9L, recordTime));
        //No more output in topic
        assertThat(outputTopic.isEmpty()).isTrue();
    }

### Testing with TestRecord Ignoring Timestamp

If the test needs to validate record header as well, but does not care about timestamp, AssertJ isEqualToIgnoringNullFields is useful.
This way the actual record timestamp can be ignored. Hamcrest also has a possibility to implement partial test with allOf 
and hasProperty matchers. 

    @Test
    public void testHeadersIgnoringTimestamp() {
        final Headers headers = new RecordHeaders(
                new Header[]{
                        new RecordHeader("foo", "value".getBytes())
                });
        //Feed 9 as key, word "Hello" as value and header to inputTopic with record timestamp filled by processing
        inputTopic.pipeInput(new TestRecord<>(9L, "Hello", headers));
        //Using isEqualToIgnoringNullFields to ignore validating recordtime
        assertThat(outputTopic.readRecord()).isEqualToIgnoringNullFields(new TestRecord<>("Hello", 9L, headers));
        assertThat(outputTopic.isEmpty()).isTrue();
    }

## Testing Collection of Records

### Testing with Value and KeyValue List

Similarly, as single record, the test can pipe in Value list and validate output with single record methods, like before,
or use readValueToList method and see the big picture when validating the whole collection at the same time. 
In this case, when the test pipes in the values, it needs to validate the keys and that way use readKeyValueToList method. 

    @Test
    public void testKeyValueList() {
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<String, Long>> expected = new LinkedList<>();
        for (final String s : inputList) {
            //Expected list contains original values as keys
            expected.add(new KeyValue<>(s, null));
        }
        //Pipe in value list
        inputTopic.pipeValueList(inputList);
        assertThat(outputTopic.readKeyValuesToList()).hasSameElementsAs(expected);
    }

### Testing with Value List and with Auto Advanced Record Time

This test is writing the KeyValue list, and as it has created OutputTopic with base time and auto-advance, each
TestRecord has a new time value.

    @Test
    public void testRecordList() {
        final List<String> inputList = Arrays.asList("This", "is", "an", "example");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        final List<TestRecord<String, Long>> expected = new LinkedList<>();
        long i = 1;
        Instant recordTime = Instant.parse("2019-06-01T10:00:00Z");
        for (final String s : inputList) {
            input.add(new KeyValue<>(i, s));
            //Excepted entries have key and value swapped and recordTime advancing 1 minute in each
            expected.add(new TestRecord<>(s, i++, recordTime));
            recordTime = recordTime.plus(advance1Min);
            i++;
        }
        //Pipe in KeyValue list
        inputTopic.pipeKeyValueList(input);
        assertThat(outputTopic.readRecordsToList()).hasSameElementsAs(expected);
    }

### Testing Reading KeyValues to Map

If the test is validating stream where only the latest record of the same key is valid, like aggregation, it can use 
readKeyValuesToMap and validate it.
The difference with Kafka KTable is that when tombstone (record with the null value) is removing the whole key, this map still 
contains keys with the null value.

    @Test
    public void testValueMap() {
        final List<String> inputList = Arrays.asList("a", "b", "c", "a", "b");
        final List<KeyValue<Long, String>> input = new LinkedList<>();
        long i = 1;
        for (final String s : inputList) {
            input.add(new KeyValue<>(i++, s));
        }
        //Pipe in KeyValue list
        inputTopic.pipeKeyValueList(input);
        //map contain the last index of each entry
        assertThat(outputTopic.readKeyValuesToMap()).hasSize(3)
                .containsEntry("a", 4L)
                .containsEntry("b", 5L)
                .containsEntry("c", 3L);
    }

The full example code can be found [here](src/test/java/com/github/jukkakarvanen/kafka/streams/example/MappingStreamAppTest.java).

## Migration

If you are migrating existing TopologyTestDriver test, you can get far with a simple find and replace approach.
The time handling is modified to use the Instant and Duration classes, but there is a possibility to use long timestamp with TestRecord. 
You can find a lot of examples of modified tests from Apache Kafka github in the diff of 
[commit in KIP-470](https://github.com/apache/kafka/commit/a5a6938c69f4310f7ec519036f0df77d8022326a).   

If you want to use these topic classes with older Kafka versions, there is a separate package which can be
used with older versions by modifying only the package import. See more info: https://github.com/jukkakarvanen/kafka-streams-test-topics 

## Testing Examples in Kafka Github:

* [WordCountDemoTest.java](https://github.com/apache/kafka/blob/trunk/streams/examples/src/test/java/org/apache/kafka/streams/examples/wordcount/WordCountDemoTest.java) 
* [TestTopicsTest.java](https://github.com/apache/kafka/blob/trunk/streams/test-utils/src/test/java/org/apache/kafka/streams/TestTopicsTest.java) 

Happy testing.