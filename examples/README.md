# Testing Kafka Streams using TestInputTopic and TestOutputTopic

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


There exist also [Fluent Kafka Streams Tests](https://medium.com/bakdata/fluent-kafka-streams-tests-e641785171ec) 
wrapper around TopologyTestDriver, but I prefer using same assertion framework across all tests. Kafka Streams itself
is using [Hamcrest](http://hamcrest.org/JavaHamcrest/tutorial), but I like a [AssertJ](https://assertj.github.io/doc/)
with it's ease of use what auto-completion in IDE is offering. This page provides examples how to write Kafka Streams test with AssertJ.  

## Setup

The general info about testing can be found from [Kafka Streams - Developer Guide](https://kafka.apache.org/documentation/streams/developer-guide/testing.html).
The first step in your test class is to prepare create TopologyTestDriver and related TestInputTopic and 
TestOutputTopic. These TestInputTopic and TestOutputTopic are new since Kafka version 2.4.0. If you do not need to test 
the record time you can create InputTopic without base time and advance information.

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

## Testing one record at a time
### Testing record value

In this example the topology we are testing is swapping key as a value and value as a key. 
That purpose we are piping the key and value, but there is a possibility to write only value.
In this example we are asserting only the value, so we use readValue method for it. 

    @Test
    public void testOnlyValue() {
        //Feed 9 as key and word "Hello" as value to inputTopic
        inputTopic.pipeInput(9L, "Hello");
        //Read value and validate it, ignore validation of kafka key, timestamp is irrelevant in this case
        assertThat(outputTopic.readValue()).isEqualTo(9L);
        //No more output in topic
        assertThat(outputTopic.isEmpty()).isTrue();
    }

At the end of the test isEmpty is assuring the are no more messages in the topic. Earlier before 2.4.0 version you 
read the record and validated the returned record was null. Now if you read from empty topic, it is generating exception. 
That way you validate wirh readValue also if you are expecting get null value.
Also now the reading from non-existing topic is causing exception instead of null value as earlier.

    @Test
    public void testReadFromEmptyTopic() {
        //Reading from empty topic generate Exception
        assertThatExceptionOfType(NoSuchElementException.class).isThrownBy(() -> { outputTopic.readValue(); })
                .withMessage("Empty topic: %s", MappingStreamApp.OUTPUT_TOPIC);
    }

### Testing KeyValue

If you need to validate both key and value you can use readKeyValue and assert it against KeyValue.

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

If you need to validate also record time, you can use readRecord and assert it against TestRecord. TestRecord
contructors support both Instant or old way with long timestamps.

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

### Testing with TestRecord ignoring timestamp

If yoy need to validate also record header, but do not care about timestamp AssertJ isEqualToIgnoringNullFields is useful.
This way actual record timestamp can be ignored. Hamcrest has also possibility to implement partial test with allOf 
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

## Testing collection of records

### Testing with ValueList and KeyValueList

Similarly than single record you can pipe in Value list and you can validate output with single record methods as earlier
or us reaValueToList method and seeing the big picture when validating the whole collection at the same time. 
In this case when we piped in values, we need to validate keys and that way of readKeyValueToList method. 

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

### Testing with ValueList and with auto advanced record time

This test is writing KeyValue list and as we have created OutputTopic with base time and auto-advance each
TestRecord has new time value.

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

### Testing reading KeyValues to Map

If you are testing stream where only the latest record of same key is valid like aggregation, you can use 
readKeyValuesToMap and validate it.
The difference with Kafka KTable is that when tombstone (record with null value) is removing the whole key, this map still 
contains keys with null value.

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

## Migration

If you are migrating existing TopologyTestDriver test, you can get far with a simple find and replace approach.
The time handling is modified to use Instant and Duration classes, but there are possibility to use long timestamp with TestRecord. 
You can find a lot of examples of modified test from Apache Kafka github in the diff of 
[commit in KIP-470](https://github.com/apache/kafka/commit/a5a6938c69f4310f7ec519036f0df77d8022326a)   

If you want to use this topic classes with older Kafka version there are separate package which can be
used with older Kafka version only modifing the package import. See more info: https://github.com/jukkakarvanen/kafka-streams-test-topics 

## Testing examples in Kafka Github:

* [WordCountDemoTest.java](https://github.com/apache/kafka/blob/trunk/streams/examples/src/test/java/org/apache/kafka/streams/examples/wordcount/WordCountDemoTest.java) 
* [TestTopicsTest.java](https://github.com/apache/kafka/blob/trunk/streams/test-utils/src/test/java/org/apache/kafka/streams/TestTopicsTest.java) 

Happy testing.