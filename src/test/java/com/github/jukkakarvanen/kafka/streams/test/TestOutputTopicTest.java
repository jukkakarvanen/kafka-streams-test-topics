/*
 * Copyright 2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.github.jukkakarvanen.kafka.streams.test;

import org.apache.kafka.common.errors.SerializationException;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Produced;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;

/**
 * Unit Test of TestOutputTopic
 * @see TestOutputTopic
 * @author Jukka Karvanen / jukinimi.com
 */

public class TestOutputTopicTest {

    private final static String INPUT_TOPIC = "input";
    private final static String OUTPUT_TOPIC = "output1";
    private final static String INPUT_TOPIC_MAP = OUTPUT_TOPIC;
    private final static String OUTPUT_TOPIC_MAP = "output2";

    private TopologyTestDriver testDriver;
    private final Serde<String> stringSerde = new Serdes.StringSerde();
    private final Serde<Long> longSerde = new Serdes.LongSerde();
    private final StringDeserializer stringDeserializer = new StringDeserializer();
    private final StringSerializer stringSerializer = new StringSerializer();

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        TestStream app = new TestStream();
        //Create Actual Stream Processing pipeline
        builder.stream(INPUT_TOPIC).to(OUTPUT_TOPIC);
        final KStream<Long, String> source = builder.stream(INPUT_TOPIC_MAP, Consumed.with(longSerde, stringSerde));
        KStream<String, Long> mapped = source.map((key, value) -> new KeyValue<>(value, key));
        mapped.to(OUTPUT_TOPIC_MAP, Produced.with(stringSerde, longSerde));
        testDriver = new TopologyTestDriver(builder.build(), app.config);
    }

    @After
    public void tearDown() {
        try {
            testDriver.close();
        } catch (final RuntimeException e) {
            // https://issues.apache.org/jira/browse/KAFKA-6647 causes exception when executed in Windows, ignoring it
            // Logged stacktrace cannot be avoided
            System.out.println("Ignoring exception, test failing in Windows due this exception:" + e.getLocalizedMessage());
        }
    }

    @Test
    public void testReadValue() {
        TestInputTopic<String, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC, stringSerde, stringSerde);
        TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC, stringSerde, stringSerde);
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        assertThat(outputTopic.readValue(), equalTo("Hello"));
        //No more output in topic
        assertThat(outputTopic.readRecord(), nullValue());
    }

    @Test
    public void testValueList() {
        //Note using here string key serde even other topic is expecting long
        //Does not affect when key not used
        TestInputTopic<String, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC, stringSerializer, stringSerializer);
        TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC, stringDeserializer, stringDeserializer);
        List<String> inputList = Arrays.asList("This", "is", "an", "example");
        //Feed list of words to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeValueList(inputList);
        List<String> output = outputTopic.readValuesToList();
        assertThat(output, hasItems("This", "is", "an", "example"));
        assertThat(output, is(equalTo(inputList)));
    }

    @Test
    public void testReadKeyValue() {
        TestInputTopic<Long, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC, longSerde, stringSerde);
        TestOutputTopic<Long, String> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC, longSerde, stringSerde);
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic.readRecord(), nullValue());
    }

    @Test
    public void testKeyValueList() {
        TestInputTopic<Long, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC_MAP, longSerde, stringSerde);
        TestOutputTopic<String, Long> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC_MAP, stringSerde, longSerde);
        List<String> inputList = Arrays.asList("This", "is", "an", "example");
        List<KeyValue<Long, String>> input = new LinkedList<>();
        List<KeyValue<String, Long>> expected = new LinkedList<>();
        long i = 0;
        for (String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.add(new KeyValue<>(s, i));
            i++;
        }
        inputTopic.pipeKeyValueList(input);
        List<KeyValue<String, Long>> output = outputTopic.readKeyValuesToList();
        assertThat(output, is(equalTo(expected)));
    }

    @Test
    public void testKeyValuesToMap() {
        TestInputTopic<Long, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC_MAP, longSerde, stringSerde);
        TestOutputTopic<String, Long> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC_MAP, stringSerde, longSerde);
        List<String> inputList = Arrays.asList("This", "is", "an", "example");
        List<KeyValue<Long, String>> input = new LinkedList<>();
        Map<String, Long> expected = new HashMap<>();
        long i = 0;
        for (String s : inputList) {
            input.add(new KeyValue<>(i, s));
            expected.put(s, i);
            i++;
        }
        inputTopic.pipeKeyValueList(input);
        Map<String, Long> output = outputTopic.readKeyValuesToMap();
        assertThat(output, is(equalTo(expected)));
    }

    @Test
    public void testMultipleTopics() {
        TestInputTopic<Long, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC, longSerde, stringSerde);
        TestOutputTopic<Long, String> outputTopic1 = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC, longSerde, stringSerde);
        TestOutputTopic<String, Long> outputTopic2 = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC_MAP, stringSerde, longSerde);
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic1.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
        assertThat(outputTopic2.readKeyValue(), equalTo(new KeyValue<>("Hello", 1L)));
        assertThat(outputTopic1.readRecord(), nullValue());
        assertThat(outputTopic2.readRecord(), nullValue());
    }

    @Test
    public void testNonExistingTopic() {
        TestOutputTopic<Long, String> outputTopic = new TestOutputTopic<>(testDriver, "no-exist", longSerde, stringSerde);
        assertThat(outputTopic.readRecord(), nullValue());
        //I was expecting this to throw an error, but returning now only null
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateTopicWithNullTopicName() {
        TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(testDriver, null, stringSerde, stringSerde);
    }

    @Test(expected = NullPointerException.class)
    public void shouldNotAllowToCreateWithNullDriver() {
        TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(null, OUTPUT_TOPIC, stringSerde, stringSerde);
    }

    @Test(expected = SerializationException.class)
    public void testWrongSerde() {
        TestInputTopic<Long, String> inputTopic = new TestInputTopic<>(testDriver, INPUT_TOPIC_MAP, longSerde, stringSerde);
        TestOutputTopic<Long, String> outputTopic = new TestOutputTopic<>(testDriver, OUTPUT_TOPIC_MAP, longSerde, stringSerde);
        inputTopic.pipeInput(1L, "Hello");
        assertThat(outputTopic.readKeyValue(), equalTo(new KeyValue<>(1L, "Hello")));
    }

    @Test
    public void testToString() {
        TestOutputTopic<String, String> outputTopic = new TestOutputTopic<>(testDriver, "topicName", stringSerde, stringSerde);
        assertThat(outputTopic.toString(), equalTo("TestOutputTopic{topic='topicName'}"));
    }
}
