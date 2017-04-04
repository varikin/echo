/*
 * Copyright 2017 Target, Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License")
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package com.netflix.spinnaker.echo.scheduler.actions.pipeline.impl

import com.netflix.spinnaker.echo.config.KafkaConfig
import com.netflix.spinnaker.echo.model.Pipeline
import com.netflix.spinnaker.echo.model.Trigger
import com.netflix.spinnaker.echo.pipelinetriggers.PipelineCache
import com.netflix.spinnaker.echo.pipelinetriggers.orca.PipelineInitiator
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.boot.autoconfigure.condition.ConditionalOnExpression
import org.springframework.boot.context.properties.EnableConfigurationProperties
import org.springframework.stereotype.Component

/**
 *
 */
// TODO (jcs) setup the properties
@Component
@ConditionalOnExpression('${kafkaConsumer.enabled:false}')
@EnableConfigurationProperties(KafkaConfig)
@Slf4j
class KafkaPollingAgent extends AbstractPollingAgent {
  public static final String TRIGGER_TYPE = Trigger.Type.KAFKA.toString()

  private final long intervalMs
  private final GaugeService gaugeService
  private final PipelineCache pipelineCache
  private final PipelineInitiator pipelineInitiator
  private final Map<String, KafkaConsumer<String, String>> consumers

  @Autowired
  KafkaPollingAgent(GaugeService gaugeService,
                    PipelineCache pipelineCache,
                    PipelineInitiator pipelineInitiator,
                    KafkaConfig kafkaConfig) {
    this.intervalMs = kafkaConfig.pollingIntervalMs
    this.gaugeService = gaugeService
    this.pipelineCache = pipelineCache
    this.pipelineInitiator = pipelineInitiator

    log.info("Setting up KafkaPollingAgent")
    consumers = kafkaConfig.brokers.collectEntries { broker ->
      def config = [
        "bootstrap.servers": broker.address,
        "group.id": "kafka-spinnaker",
        "key.deserializer": StringDeserializer.class.getName(),
        "value.deserializer": StringDeserializer.class.getName(),
        // TODO (jcs) set cert if exists
      ]
      return [broker.name, new KafkaConsumer<String, String>(config)]
    }
  }

  @Override
  String getName() {
    return PipelineConfigsPollingAgent.class.simpleName
  }

  @Override
  long getIntervalMs() {
    return intervalMs
  }

  @Override
  void execute() {
    long start = System.currentTimeMillis()

    try {
      log.info("Running the Kafka polling agent")

      Map<String, Map<String, Set<Pipeline>>> pipelines = getPipelines()

      subscribe(pipelines)

      this.consumers.each { name, consumer ->
        try {
          if (consumer.listTopics().isEmpty()) {
            log.debug("Kafka consumer ${name} is not subscribed to any topics")
            return
          }
          List<ConsumerRecord<String, String>> records = consumer.poll(0)
          records.each { record ->
            def topic = record.topic()
            def message = record.value()
            pipelines.get(name).get(topic).each { pipeline ->
              def pipelineWithContext = buildPipelineWithContext(pipeline, message)
              pipelineInitiator.call(pipelineWithContext)
            }
          }
        } catch (Exception e) {
          log.error("Exception occurred while polling Kafka server {}, continuing to the next Kafka server", name, e)
        }
      }
    } catch (Exception e) {
      log.error("Exception occurred while polling Kafka", e)
    } finally {
      gaugeService.submit("kafkaPollingAgent.executionTimeMillis", (double) System.currentTimeMillis() - start)
    }
  }

  /**
   * Returns pipelines with with Kafka triggers.
   *
   * The collection is a Map of maps. The outer map is server names to a map of topics to a set of pipelines.
   *
   * In other words, a server has many topics, each topic has many pipelines. For example:
   *
   * [
   *  production-kafka: [
   *    application: [app-pipeline, app-pipeline-2],
   *    app2: [app2-pipeline, app-pipeline-2]
   *  ],
   *  testing-kafka: [
   *    application: [app-test-pipeline, app-test-pipeline-2],
   *    app2: [app2-test-pipeline, app-test-pipeline-2]
   *  ]
   * ]
   */
  private Map<String, Map<String, Set<Pipeline>>> getPipelines() {
    // Get pipelines that have a Kafka trigger
    def pipelines = pipelineCache.getPipelines().findAll { pipeline ->
      pipeline.triggers && pipeline.triggers.any { TRIGGER_TYPE.equalsIgnoreCase(it.type) }
    }

    // Build the map of servers to topics and pipelines
    Map<String, Map<String, Set<Pipeline>>> kafkaToPipeline = [:]
    pipelines.each { pipeline ->
      def server = pipeline.properties['kafkaServer']
      def topic = pipeline.properties['kafkaTopic']
      def entry = kafkaToPipeline.get(server, [:].withDefault { new HashSet<Pipeline>() })
      entry[topic].add(pipeline)
    }
    return kafkaToPipeline
  }

  /**
   * Subscribe to all Kafka topics specified by pipelines.
   *
   * This is an idempotent method in that it will compare the current state against the needed state.
   * Then it will resubcribe to all Kafka topics if there is a difference.
   *
   * @param pipelines list of pipelines configured to be triggered via Kafka
   */
  private void subscribe(Map<String, Map<String, Pipeline>> pipelines) {

    def currentState = [:]
    consumers.each{ name, consumer ->
      currentState[name] = consumer.subscription()
    }

    pipelines.each { name, topics ->
      def requiredTopics = topics.keySet()
      if (requiredTopics != currentState[name]) {
          consumers[name].subscribe(requiredTopics)
      }
    }
  }

  // TODO (jcs) build the trigger!
  private Pipeline buildPipelineWithContext(Pipeline pipeline, String message) {
    Trigger trigger = Trigger.builder()
      .type(TRIGGER_TYPE)
      .enabled(true)
      .build()
    return pipeline.withTrigger(trigger)
  }
}

