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
import org.apache.kafka.common.annotation.InterfaceStability;
import org.apache.kafka.common.record.TimestampType;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serializer;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.processor.ProcessorContext;
import org.apache.kafka.streams.processor.PunctuationType;
import org.apache.kafka.streams.processor.Punctuator;
import org.apache.kafka.streams.processor.StateStore;
import org.apache.kafka.streams.test.ConsumerRecordFactory;
import org.apache.kafka.streams.test.OutputVerifier;
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
 * This class makes it easier to write tests to verify the behavior of topologies created with {@link Topology} or
 * {@link StreamsBuilder}.
 * You can test simple topologies that have a single processor, or very complex topologies that have multiple sources,
 * processors, sinks, or sub-topologies.
 * Best of all, the class works without a real Kafka broker, so the tests execute very quickly with very little overhead.
 * <p>
 * Using the {@code TopologyTestDriver} in tests is easy: simply instantiate the driver and provide a {@link Topology}
 * (cf. {@link StreamsBuilder#build()}) and {@link Properties configs}, create {@link #createInputTopic(String, Serde, Serde)}
 * and use the {@link TestInputTopic} to supply an input message to the topology,
 * and then create {@link #createOutputTopic(String, Serde, Serde)} and use the {@link TestOutputTopic} to read and
 * verify any messages output by the topology.
 * <p>
 * Although the driver doesn't use a real Kafka broker, it does simulate Kafka {@link Consumer consumers} and
 * {@link Producer producers} that read and write raw {@code byte[]} messages.
 * You can let {@link TestInputTopic} and {@link TestOutputTopic} to handle conversion
 * form regular Java objects to raw bytes.
 *
 * <h2>Driver setup</h2>
 * In order to create a {@code TopologyTestDriver} instance, you need a {@link Topology} and a {@link Properties config}.
 * The configuration needs to be representative of what you'd supply to the real topology, so that means including
 * several key properties (cf. {@link StreamsConfig}).
 * For example, the following code fragment creates a configuration that specifies a local Kafka broker list (which is
 * needed but not used), a timestamp extractor, and default serializers and deserializers for string keys and values:
 *
 * <pre>{@code
 * Properties props = new Properties();
 * props.setProperty(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9091");
 * props.setProperty(StreamsConfig.DEFAULT_TIMESTAMP_EXTRACTOR_CLASS_CONFIG, CustomTimestampExtractor.class.getName());
 * props.setProperty(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * props.setProperty(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
 * Topology topology = ...
 * TopologyTestDriver driver = new TopologyTestDriver(topology, props);
 * }</pre>
 *
 * <h2>Processing messages</h2>
 * <p>
 * Your test can supply new input records on any of the topics that the topology's sources consume.
 * This test driver simulates single-partitioned input topics.
 * Here's an example of an input message on the topic named {@code input-topic}:
 *
 * <pre>
 * TestInputTopic<String, String> inputTopic = driver.createInputTopic("input-topic", stringSerde, stringSerde);
 * inputTopic.pipeInput("key1", "value1");
 * </pre>
 *
 * When {@link TestInputTopic#pipeInput(Object, Object)} is called, the driver passes the input message through to the appropriate source that
 * consumes the named topic, and will invoke the processor(s) downstream of the source.
 * If your topology's processors forward messages to sinks, your test can then consume these output messages to verify
 * they match the expected outcome.
 * For example, if our topology should have generated 2 messages on {@code output-topic-1} and 1 message on
 * {@code output-topic-2}, then our test can obtain these messages using the
 * {@link TestOutputTopic#readKeyValue()}  method:
 *
 * <pre>{@code
 * TestOutputTopic<String, String> outputTopic1 = driver.createOutputTopic("output-topic-1", stringSerde, stringSerde);
 * TestOutputTopic<String, String> outputTopic2 = driver.createOutputTopic("output-topic-2", stringSerde, stringSerde);
 *
 * KeyValue<String, String> record1 = outputTopic1.readKeyValue();
 * KeyValue<String, String> record2 = outputTopic1.readKeyValue();
 * KeyValue<String, String> record3 = outputTopic1.readKeyValue();
 * }</pre>
 *
 * Again, our example topology generates messages with string keys and values, so we supply our string deserializer
 * instance for use on both the keys and values. Your test logic can then verify whether these output records are
 * correct.
 * <p>
 * Note, that calling {@code pipeInput()} will also trigger {@link PunctuationType#STREAM_TIME event-time} base
 * {@link ProcessorContext#schedule(Duration, PunctuationType, Punctuator) punctuation} callbacks.
 * However, you won't trigger {@link PunctuationType#WALL_CLOCK_TIME wall-clock} type punctuations that you must
 * trigger manually via {@link #advanceWallClockTime(long)}.
 * <p>
 * Finally, when completed, make sure your tests {@link #close()} the driver to release all resources and
 * {@link org.apache.kafka.streams.processor.Processor processors}.
 *
 * <h2>Processor state</h2>
 * <p>
 * Some processors use Kafka {@link StateStore state storage}, so this driver class provides the generic
 * {@link #getStateStore(String)} as well as store-type specific methods so that your tests can check the underlying
 * state store(s) used by your topology's processors.
 * In our previous example, after we supplied a single input message and checked the three output messages, our test
 * could also check the key value store to verify the processor correctly added, removed, or updated internal state.
 * Or, our test might have pre-populated some state <em>before</em> submitting the input message, and verified afterward
 * that the processor(s) correctly updated the state.
 *
 * @see TestInputTopic
 * @see TestOutputTopic
 */
@InterfaceStability.Evolving
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
    public TopologyTestDriver(final Topology topology,
                              final Properties config,
                              final long initialWallClockTimeMs) {
        super(topology, config, initialWallClockTimeMs);
    }

    /**
     * Create a new test diver instance.
     *
     * @param topology               the topology to be tested
     * @param config                 the configuration for the topology
     * @param initialWallClockTime   the initial value of internally mocked wall-clock time
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
        ProducerRecord<byte[], byte[]> record = readOutput(topicName);
        if (record != null) {
         outputRecordsByTopic.computeIfAbsent(topicName, k -> new LinkedList<>()).add(record);
        }
        final Queue<ProducerRecord<byte[], byte[]>> outputRecords = outputRecordsByTopic.get(topicName);
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
     * @param topicName             the name of the topic
     * @param keySerde   the serde for the key type
     * @param valueSerde the serde for the value type
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serde<K> keySerde,
                                                              final Serde<V> valueSerde) {
        return new TestInputTopic<K, V>(this, topicName, keySerde, valueSerde);
    }

    /**
     * Create {@link TestInputTopic} to be used for piping records to topic
     * Uses provided start timestamp and autoAdvance parameter for records
     *
     * @param topicName             the name of the topic
     * @param keySerde   the serde for the key type
     * @param valueSerde the serde for the value type
     * @param startTimestamp Start timestamp for auto-generated record time
     * @param autoAdvance autoAdvance duration for auto-generated record time
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestInputTopic} object
     */
    public final <K, V> TestInputTopic<K, V> createInputTopic(final String topicName,
                                                              final Serde<K> keySerde,
                                                              final Serde<V> valueSerde,
                                                              final Instant startTimestamp,
                                                              final Duration autoAdvance) {
        return new TestInputTopic<K, V>(this, topicName, keySerde, valueSerde, startTimestamp, autoAdvance);
    }

    /**
     * Create {@link TestOutputTopic} to be used for reading records from topic
     *
     * @param topicName             the name of the topic
     * @param keySerde   the serde for the key type
     * @param valueSerde the serde for the value type
     * @param <K> the key type
     * @param <V> the value type
     * @return {@link TestOutputTopic} object
     */
    public final <K, V> TestOutputTopic<K, V> createOutputTopic(final String topicName,
                                                                final Serde<K> keySerde,
                                                                final Serde<V> valueSerde) {
        return new TestOutputTopic<K, V>(this, topicName, keySerde, valueSerde);
    }


    /**
     * Read the next record from the given topic.
     * These records were output by the topology during the previous calls to {@link #pipeInput(ConsumerRecord)}.
     *
     * @param topic             the name of the topic
     * @param keyDeserializer   the deserializer for the key type
     * @param valueDeserializer the deserializer for the value type
     * @param <K> the key type
     * @param <V> the value type
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
     * @param <K> the key type
     * @param <V> the value type*
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
        //This is not accurate
        final Queue<ProducerRecord<byte[], byte[]>> queue = getRecordsQueue(topic);
        if (queue == null ) {
            //Return 0 if not initialized, getRecordsQueue throw exception if non existing topic
            return 0;
        }
        return queue.size();
    }
}