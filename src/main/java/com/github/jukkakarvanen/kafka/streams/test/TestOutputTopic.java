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

import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.streams.KeyValue;
import org.apache.kafka.streams.TopologyTestDriver;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * This class makes it easier to write tests with {@link TopologyTestDriver}.
 * To use {@code TestOutputTopic} create new class with topicName and correct Serdes or Deserealizers
 * In actual test code, you can read message values, keys, {@link KeyValue} or {@link ProducerRecord}
 * without needing to care serdes. You need to have own TestOutputTopic for each topic.
 *
 * If you need to test key, value and headers, use @{link #readRecord} methods.
 * Using @{link #readKeyValue} you get directly KeyValue, but have no access to headers any more
 * Using @{link #readValue} and @{link #readKey} you get directly Key or Value, but have no access to headers any more
 * Note, if using @{link #readKey} and @{link #readValue} in sequence, you get the key of first record and value of the next one
 *
 * <h2>Processing messages</h2>*
 * <pre>{@code
 *      private TestOutputTopic<String, Long> outputTopic;
 * -@Before
 *      ...
 *     outputTopic = new TestOutputTopic<String, Long>(testDriver, outputTopic, new Serdes.StringSerde(), new Serdes.LongSerde());
 *
 * -@Test
 *     ...
 *     assertThat(outputTopic.readValue()).isEqual(1);
 * </pre>
 *
 * @author Jukka Karvanen / jukinimi.com
 *
 * @param <K> the type of the key
 * @param <V> the type of the value
 *
 * @see TopologyTestDriver, ConsumerRecordFactory
 */
public class TestOutputTopic<K, V> {
    //Possibility to use in subclasses
    @SuppressWarnings({"WeakerAccess"})
    protected final TopologyTestDriver driver;
    @SuppressWarnings({"WeakerAccess"})
    protected final String topic;
    @SuppressWarnings({"WeakerAccess"})
    protected final Deserializer<K> keyDeserializer;
    @SuppressWarnings({"WeakerAccess"})
    protected final Deserializer<V> valueDeserializer;

    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestOutputTopic(final TopologyTestDriver driver,
                           final String topic,
                           final Serde<K> keySerde,
                           final Serde<V> valueSerde) {
        this(driver, topic, keySerde.deserializer(), valueSerde.deserializer());
    }

    @SuppressWarnings({"WeakerAccess", "unused"})
    public TestOutputTopic(final TopologyTestDriver driver,
                           final String topic,
                           final Deserializer<K> keyDeserializer,
                           final Deserializer<V> valueDeserializer) {
        this.driver = driver;
        this.topic = topic;
        this.keyDeserializer = keyDeserializer;
        this.valueDeserializer = valueDeserializer;
    }


    /**
     * Read one Record from output topic and return value only.
     * <p>
     * Note. The key and header is not available
     *
     * @return Next value for output topic
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public V readValue() {
        ProducerRecord<K, V> record = readRecord();
        if (record == null) return null;
        return record.value();
    }

    /**
     * Read one Record from output topic and and return key only.
     * <p>
     * Note. The value and header is not available
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public K readKey() {
        ProducerRecord<K, V> record = readRecord();
        if (record == null) return null;
        return record.key();
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public KeyValue<K, V> readKeyValue() {
        ProducerRecord<K, V> record = readRecord();
        if (record == null) return null;
        return new KeyValue<>(record.key(), record.value());
    }

    /**
     * Read one Record from output topic.
     *
     * @return Next output as ProducerRecord
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public ProducerRecord<K, V> readRecord() {
        return driver.readOutput(topic, keyDeserializer, valueDeserializer);
    }

    /**
     * Read output to map.
     * If the existing key is modified, it can appear twice in output and is replaced in map
     *
     * @return Map of output by key
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public Map<K, V> readRecordsToMap() {
        final Map<K, V> output = new HashMap<>();
        ProducerRecord<K, V> outputRow;
        while ((outputRow = readRecord()) != null) {
            output.put(outputRow.key(), outputRow.value());
        }
        return output;
    }

    /**
     * Read output to map.
     * If the existing key is modified, it can appear twice in output and is replaced in map
     *
     * @return Map of output by key
     */
    @SuppressWarnings({"WeakerAccess", "unused"})
    public List<KeyValue<K, V>> readRecordsToList() {
        final List<KeyValue<K, V>> output = new LinkedList<>();
        KeyValue<K, V> outputRow;
        while ((outputRow = readKeyValue()) != null) {
            output.add(outputRow);
        }
        return output;
    }
}
