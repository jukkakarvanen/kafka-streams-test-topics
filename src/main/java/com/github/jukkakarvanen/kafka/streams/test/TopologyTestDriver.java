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

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.time.Instant;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.Map;
import java.util.NoSuchElementException;
import java.util.Objects;
import java.util.Properties;
import java.util.Queue;

/**
 * Replacement class for {@link org.apache.kafka.streams.TopologyTestDriver} to use
 * {@link TestInputTopic} and {@link TestInputTopic} with Kafka version prior 2.4.0
 *
 * @see TestInputTopic
 * @see TestOutputTopic
 */
public class TopologyTestDriver extends org.apache.kafka.streams.TopologyTestDriver {

    private static final Logger log = LoggerFactory.getLogger(TopologyTestDriver.class);

    /**
     * Create a new test diver instance.
     * Initialized the internally mocked wall-clock time with {@link System#currentTimeMillis() current system time}.
     *
     * @param topology the topology to be tested
     * @param config   the configuration for the topology
     */
    @SuppressWarnings("WeakerAccess")
    public TopologyTestDriver(final Topology topology,
                              final Properties config) {
        super(topology, config);
    }

    /**
     * Create a new test diver instance.
     *
     * @param topology               the topology to be tested
     * @param config                 the configuration for the topology
     * @param initialWallClockTimeMs the initial value of internally mocked wall-clock time
     */
    @SuppressWarnings("WeakerAccess")
    @Deprecated
    public TopologyTestDriver(final Topology topology,
                              final Properties config,
                              final long initialWallClockTimeMs) {
        super(topology, config, initialWallClockTimeMs);
    }

    /**
     * Create a new test diver instance.
     *
     * @param topology             the topology to be tested
     * @param config               the configuration for the topology
     * @param initialWallClockTime the initial value of internally mocked wall-clock time
     */
    @SuppressWarnings("WeakerAccess")
    public TopologyTestDriver(final Topology topology,
                              final Properties config,
                              final Instant initialWallClockTime) {
        this(topology, config, initialWallClockTime == null ? System.currentTimeMillis() : initialWallClockTime.toEpochMilli());
    }


    private final Map<String, Queue<ProducerRecord<byte[], byte[]>>> outputRecordsByTopic = new HashMap<>();

    final protected Queue<ProducerRecord<byte[], byte[]>> getRecordsQueue(String topicName) {
        //This is hack because no access to private queue
        final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.computeIfAbsent(topicName, k -> new LinkedList<>());
        ProducerRecord<byte[], byte[]> record;
        while ((record = readOutput(topicName)) != null) {
            outputRecords.add(record);
        }
        return outputRecords;
    }

    /**
     * Advances the internally mocked wall-clock time.
     * This might trigger a {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type
     * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) punctuations}.
     *
     * @param advance the amount of time to advance wall-clock time
     */
    @SuppressWarnings("WeakerAccess")
    public void advanceWallClockTime(final Duration advance) {
        Objects.requireNonNull(advance, "advance cannot be null");
        advanceWallClockTime(advance.toMillis());
    }

    /**
     * Create {@link TestInputTopic} to be used for piping records to topic
     * Uses current system time as start timestamp for records.
     * Auto-advance is disabled.
     *
     * @param topicName       the name of the topic
     * @param keySerializer   the Serializer for the key type
     * @param valueSerializer the Serializer for the value type
     * @param <K>             the key type
     * @param <V>             the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serializer<K> keySerializer,
                                                              final Serializer<V> valueSerializer) {
        return new TestInputTopic<K, V>(this, topicName, keySerializer, valueSerializer, Instant.now(), Duration.ZERO);
    }

    /**
     * Create {@link TestInputTopic} to be used for piping records to topic
     * Uses provided start timestamp and autoAdvance parameter for records
     *
     * @param topicName         the name of the topic
     * @param keySerializer   the Deserializer for the key type
     * @param valueSerializer the Deserializer for the value type
     * @param startTimestamp    Start timestamp for auto-generated record time
     * @param autoAdvance       autoAdvance duration for auto-generated record time
     * @param <K>               the key type
     * @param <V>               the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serializer<K> keySerializer,
                                                              final Serializer<V> valueSerializer,
                                                              final Instant startTimestamp,
                                                              final Duration autoAdvance) {
        return new TestInputTopic<K, V>(this, topicName, keySerializer, valueSerializer, startTimestamp, autoAdvance);
    }

    /**
     * Create {@link TestOutputTopic} to be used for reading records from topic
     *
     * @param topicName         the name of the topic
     * @param keyDeserializer   the Deserializer for the key type
     * @param valueDeserializer the Deserializer for the value type
     * @param <K>               the key type
     * @param <V>               the value type
     * @return {@link TestOutputTopic} object
     */
    public final <K, V> TestOutputTopic<K, V> createOutputTopic(final String topicName,
                                                                final Deserializer<K> keyDeserializer,
                                                                final Deserializer<V> valueDeserializer) {
        return new TestOutputTopic<K, V>(this, topicName, keyDeserializer, valueDeserializer);
    }


    /**
     * Read the next record from the given topic.
     * These records were output by the topology during the previous calls to {@link #pipeInput(ConsumerRecord)}.
     *
     * @param topic             the name of the topic
     * @param keyDeserializer   the deserializer for the key type
     * @param valueDeserializer the deserializer for the value type
     * @param <K>               the key type
     * @param <V>               the value type
     * @return the next record on that topic, or {@code null} if there is no record available
     */
    @SuppressWarnings("WeakerAccess")
    <K, V> TestRecord<K, V> readRecord(final String topic,
                                       final Deserializer<K> keyDeserializer,
                                       final Deserializer<V> valueDeserializer) {
        final Queue<? extends ProducerRecord<byte[], byte[]>> outputRecords = getRecordsQueue(topic);
        if (outputRecords == null) {
            throw new NoSuchElementException("Uninitialized topic: " + topic);
        }
        final ProducerRecord<byte[], byte[]> record = outputRecords.poll();
        if (record == null) {
            throw new NoSuchElementException("Empty topic: " + topic);
        }
        final K key = keyDeserializer.deserialize(record.topic(), record.key());
        final V value = valueDeserializer.deserialize(record.topic(), record.value());
        return new TestRecord<>(key, value, record.headers(), record.timestamp());
    }

    /**
     * Serialize an input recond and send on the specified topic to the topology and then
     * commit the messages.
     *
     * @param topic           the name of the topic
     * @param record          TestRecord to be send
     * @param keySerializer   the key serializer
     * @param valueSerializer the value serializer
     * @param <K>             the key type
     * @param <V>             the value type*
     * @param time            timestamp to override the record timestamp
     */
    @SuppressWarnings("WeakerAccess")
    <K, V> void pipeRecord(final String topic,
                           final TestRecord<K, V> record,
                           final Serializer<K> keySerializer,
                           final Serializer<V> valueSerializer,
                           final Instant time) {
        ConsumerRecordFactory<K, V> factory =
                new ConsumerRecordFactory<>(topic, keySerializer, valueSerializer);
        long timestamp = (time != null) ? time.toEpochMilli() : record.timestamp();
        pipeInput(factory.create(record.key(), record.value(), record.headers(), timestamp));
    }

    final long getQueueSize(final String topic) {
        final Queue<ProducerRecord<byte[], byte[]>> queue = getRecordsQueue(topic);
        if (queue == null) {
            //Return 0 if not initialized, getRecordsQueue throw exception if non existing topic
            return 0;
        }
        return queue.size();
    }
}