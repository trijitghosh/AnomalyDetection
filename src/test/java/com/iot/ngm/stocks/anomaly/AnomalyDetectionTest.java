package com.iot.ngm.stocks.anomaly;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import io.confluent.kafka.schemaregistry.client.MockSchemaRegistryClient;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.TestInputTopic;
import org.apache.kafka.streams.TestOutputTopic;
import org.apache.kafka.streams.TopologyTestDriver;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.time.Instant;
import java.util.Collections;
import java.util.Properties;

import static org.junit.Assert.*;

public class AnomalyDetectionTest {

    AnomalyDetectionMain ad;
    MockSchemaRegistryClient mockSRClient;
    TopologyTestDriver testDriver;
    TestInputTopic<String, AggregatedStock> inputTopic;
    TestOutputTopic<String, AggregatedStock> outputTopic;

    @Before
    public void setupTopologyTestDriver(){
        ad = new AnomalyDetectionMain();
        mockSRClient = new MockSchemaRegistryClient();
        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "test");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "dummy:1234");
        config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG, "mock://dummy:5678");
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
        SpecificAvroSerde<AggregatedStock> aggStockSerde = new SpecificAvroSerde<>(mockSRClient);
        aggStockSerde.configure(Collections.singletonMap("schema.registry.url", "mock://dummy:5678"), false);
        testDriver = new TopologyTestDriver(ad.createTopology(aggStockSerde), config);
        inputTopic = testDriver.createInputTopic("stock-agg", new StringSerializer(), aggStockSerde.serializer());
        outputTopic = testDriver.createOutputTopic("stock-anomaly", new StringDeserializer(), aggStockSerde.deserializer());
    }

    @After
    public void closeTestDriver(){
        testDriver.close();
    }

    @Test
    public void streamTest() {
        assertTrue(outputTopic.isEmpty());
        // valid: hash % 100 > 10
        pipeEvent("2022-02-16T19:17:00Z", 70.0f, 70.0f, "IBM", 3.0f, 10);
        assertTrue(outputTopic.isEmpty());
        // invalid: hash % 100 <= 10
        pipeEvent("2022-02-24T10:26:15.387Z", 10.0f, 10.0f, "Amazon", 70.0f, 5);
        assertEquals(outputTopic.getQueueSize(), 1);
        outputTopic.readValue();
        assertEquals(outputTopic.getQueueSize(), 0);
        // valid: hash % 100 > 10
        pipeEvent("2020-02-16T19:17:00Z", 100.0f, 100.0f, "Apple", 2.0f, 5);
        assertTrue(outputTopic.isEmpty());
        // valid: hash % 100 > 10
        pipeEvent("2018-02-16T19:17:00Z", 130.0f, 130.0f, "Netflix", 5.0f, 5);
        assertTrue(outputTopic.isEmpty());
        // invalid: hash % 100 <= 10
        pipeEvent("2022-02-24T10:23:46.336Z", 40.0f, 40.0f, "Google", 110.0f, 5);
        assertEquals(outputTopic.getQueueSize(), 1);
        outputTopic.readValue();
        assertEquals(outputTopic.getQueueSize(), 0);
    }

    @Test
    public void stockAnomalyTest() {
        // hash % 100 <= 10
        AggregatedStock s1 = new AggregatedStock(Instant.parse("2022-02-24T10:26:15.387Z"), 10.0f, 10.0f, "Amazon", 70.0f, 5);
        AggregatedStock s2 = new AggregatedStock(Instant.parse("2022-02-24T10:23:46.336Z"), 40.0f, 40.0f, "Google", 110.0f, 5);
        assertTrue(ad.isStockAnomaly(s1));
        assertTrue(ad.isStockAnomaly(s2));
        // hash % 100 > 10
        AggregatedStock s3 = new AggregatedStock(Instant.parse("2022-02-16T19:17:00Z"), 70.0f, 70.0f, "IBM", 3.0f, 10);
        AggregatedStock s4 = new AggregatedStock(Instant.parse("2020-02-16T19:17:00Z"), 100.0f, 100.0f, "Apple", 2.0f, 5);
        AggregatedStock s5 = new AggregatedStock(Instant.parse("2018-02-16T19:17:00Z"), 130.0f, 130.0f, "Netflix", 5.0f, 5);
        assertFalse(ad.isStockAnomaly(s3));
        assertFalse(ad.isStockAnomaly(s4));
        assertFalse(ad.isStockAnomaly(s5));
    }

    private void pipeEvent(String time, float min_open, float max_close, String symbol, float variation, int interval) {
        AggregatedStock s = new AggregatedStock(Instant.parse(time), min_open, max_close, symbol, variation, interval);
        inputTopic.pipeInput(s.getSymbol(), s);
    }

}
