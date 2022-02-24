package com.iot.ngm.stocks.anomaly;

import com.typesafe.config.Config;
import java.util.List;

/**
 * Application configuration properties.
 */
public class AppConfig {

  private final String bootstrapServers;
  private final String schemaRegistryUrl;
  private final List<String> inputTopicsName;
  private final String outputTopicName;
  private final String applicationId;
  private final int anomalyPercentage;

  /**
   * Constructor.
   *
   * @param config config file
   */
  public AppConfig(Config config) {
    this.bootstrapServers = config.getString("kafka.bootstrap.servers");
    this.schemaRegistryUrl = config.getString("kafka.schema.registry.url");
    this.inputTopicsName = config.getStringList("kafka.input.topics.name");
    this.outputTopicName = config.getString("kafka.output.topic.name");
    this.applicationId = config.getString("app.application.id");
    this.anomalyPercentage = config.getInt("app.anomaly.percentage");
  }

  /**
   * Get kafka.bootstrap.servers property
   *
   * @return bootstrap servers property
   */
  public String getBootstrapServers() {
    return bootstrapServers;
  }

  /**
   * Get kafka.schema.registry.url property
   *
   * @return schema registry url property
   */
  public String getSchemaRegistryUrl() {
    return schemaRegistryUrl;
  }

  /**
   * Get kafka.input.topics.name property
   *
   * @return input topics name property
   */
  public List<String> getInputTopicsName() {
    return inputTopicsName;
  }

  /**
   * Get kafka.output.topic.name property
   *
   * @return output topic name property
   */
  public String getOutputTopicName() {
    return outputTopicName;
  }

  /**
   * Get app.application.id property
   *
   * @return application id (for Kafka Streams) property
   */
  public String getApplicationId() {
    return applicationId;
  }

  /**
   * Get app.anomaly.percentage property
   *
   * @return anomaly percentage ratio property
   */
  public int getAnomalyPercentage() {
    return anomalyPercentage;
  }

}
