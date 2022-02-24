package com.iot.ngm.stocks.anomaly;

import com.iot.ngm.stocks.dtos.AggregatedStock;
import com.typesafe.config.ConfigFactory;
import io.confluent.kafka.serializers.KafkaAvroSerializerConfig;
import io.confluent.kafka.streams.serdes.avro.SpecificAvroSerde;
import java.io.IOException;
import java.util.Collections;
import java.util.Properties;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.common.utils.Utils;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.Consumed;
import org.apache.kafka.streams.kstream.Produced;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

/**
 * Kafka Streams application to detect price variation anomalies.
 */
public class AnomalyDetectionMain {

  private static Logger log;
  private static AppConfig appConfig;

  /**
   * Constructor.
   */
  public AnomalyDetectionMain() {
    log = LoggerFactory.getLogger(AnomalyDetectionMain.class.getSimpleName());
    appConfig = new AppConfig(ConfigFactory.load());
  }

  /**
   * Main method. Sets Serde, creates Kafka Streams topology and config, and starts Kafka Streams.
   *
   * @param args input arguments (unused)
   */
  public static void main(String[] args) {
    AnomalyDetectionMain ad = new AnomalyDetectionMain();
    SpecificAvroSerde<AggregatedStock> aggStockSerde = new SpecificAvroSerde<>();
    aggStockSerde.configure(
        Collections.singletonMap(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
            appConfig.getSchemaRegistryUrl()), false);
    KafkaStreams kafkaStreams =
        new KafkaStreams(ad.createTopology(aggStockSerde), ad.getStreamsConfig());
    //kafkaStreams.cleanUp();
    kafkaStreams.start();
    Runtime.getRuntime().addShutdownHook(new Thread(kafkaStreams::close));
  }

  /**
   * Sets Kafka Streams properties.
   *
   * @return stream config properties
   */
  public Properties getStreamsConfig() {
    Properties config = new Properties();
    config.put(StreamsConfig.APPLICATION_ID_CONFIG, appConfig.getApplicationId());
    config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, appConfig.getBootstrapServers());
    config.put(KafkaAvroSerializerConfig.SCHEMA_REGISTRY_URL_CONFIG,
        appConfig.getSchemaRegistryUrl());
    config.put(StreamsConfig.PROCESSING_GUARANTEE_CONFIG, StreamsConfig.EXACTLY_ONCE_V2);
    config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.StringSerde.class.getName());
    config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, SpecificAvroSerde.class.getName());
    return config;
  }

  /**
   * Creates the topology for Kafka Streams.
   * 1: consumes data from input topic
   * 2: drops non anomaly records
   * 3: appends records to application log
   * 4: produces data to output topic
   *
   * @param aggStockSerde AggregatedStock Serde.
   * @return stream topology
   */
  public Topology createTopology(SpecificAvroSerde<AggregatedStock> aggStockSerde) {
    StreamsBuilder builder = new StreamsBuilder();
    Serdes.StringSerde stringSerde = new Serdes.StringSerde();
    builder.stream(appConfig.getInputTopicsName(), Consumed.with(stringSerde, aggStockSerde))
        // drop non anomaly records
        .filter((k, aggStock) -> isStockAnomaly(aggStock))
        // append record to application log
        .peek((k, aggStock) -> log.info(
            "[ANOMALY]: variation=" + aggStock.getVariation() + " | symbol=" + aggStock.getSymbol()
                + " | interval=" + aggStock.getInterval() + " | min_open=" + aggStock.getMinOpen()
                + " | max_close=" + aggStock.getMaxClose() + " | time=" + aggStock.getTime()))
        // write to output topic
        .to(appConfig.getOutputTopicName(), Produced.with(stringSerde, aggStockSerde));
    return builder.build();
  }

  /**
   * Hash to randomize the sample, with x (app.anomaly.percentage) % of stocks flagged as anomalies.
   * Should be replaced with ML model.
   *
   * @param stock stock to check for anomaly
   * @return true, for an anomaly; false, otherwise
   */
  public boolean isStockAnomaly(AggregatedStock stock) {
    try {
      int hash = Utils.toPositive(Utils.murmur2(stock.toByteBuffer().array()));
      return (hash % 100) <= appConfig.getAnomalyPercentage();
    } catch (IOException e) {
      return false;
    }
  }

}
