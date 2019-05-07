/*
 * Copyright 2019 the original author or authors.
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

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.TopologyTestDriver;
import org.hamcrest.MatcherAssert;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.hamcrest.CoreMatchers.equalTo;
import static org.hamcrest.CoreMatchers.hasItems;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.CoreMatchers.nullValue;
import static org.hamcrest.MatcherAssert.assertThat;
import static org.junit.Assert.assertNull;

/**
 * Unit test of {@link SimpleStreamApp} stream using TopologyTestDriver.
 */
public class SimpleStramAppTest {

    private TopologyTestDriver testDriver;
    private TestInputTopic<String, String> inputTopic;
    private TestOutputTopic<String, String> outputTopic;

    @Before
    public void setup() {
        final StreamsBuilder builder = new StreamsBuilder();
        //Create Actual Stream Processing pipeline
        SimpleStreamApp.createStream(builder);
        testDriver = new TopologyTestDriver(builder.build(), SimpleStreamApp.getStreamsConfig());
        inputTopic = new TestInputTopic<String, String>(testDriver, SimpleStreamApp.INPUT_TOPIC, new Serdes.StringSerde(), new Serdes.StringSerde());
        outputTopic = new TestOutputTopic<String, String>(testDriver, SimpleStreamApp.OUTPUT_TOPIC, new Serdes.StringSerde(), new Serdes.StringSerde());
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
    public void testOneWord() {
        //Feed word "Hello" to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeInput("Hello");
        MatcherAssert.assertThat(outputTopic.readValue(), equalTo("Hello"));
        //No more output in topic
        MatcherAssert.assertThat(outputTopic.readRecord(), nullValue());
    }

    @Test
    public void testListWord() {
        List<String> inputList = Arrays.asList("This", "is", "an", "example");
        //Feed list of words to inputTopic and no kafka key, timestamp is irrelevant in this case
        inputTopic.pipeValueList(inputList);
        List<String> output = outputTopic.readValuesToList();
        MatcherAssert.assertThat(output, hasItems("This", "is", "an", "example"));
        MatcherAssert.assertThat(output.size(), equalTo(inputList.size()));
    }

}
