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

import com.netflix.spinnaker.echo.model.Pipeline
import com.netflix.spinnaker.echo.pipelinetriggers.PipelineCache
import groovy.util.logging.Slf4j
import org.apache.kafka.clients.consumer.ConsumerRecord
import org.apache.kafka.clients.consumer.KafkaConsumer
import org.apache.kafka.common.serialization.StringDeserializer
import org.springframework.beans.factory.annotation.Autowired
import org.springframework.beans.factory.annotation.Value
import org.springframework.boot.actuate.metrics.GaugeService
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty
import org.springframework.stereotype.Component

/**
 *
 */
// TODO (jcs) setup the properties
@Component
@ConditionalOnProperty('${kafka.enabled}')
@Slf4j
class KafkaPollingAgent extends AbstractPollingAgent {
  public static final String TRIGGER_TYPE = "kafka"

  private final long intervalMs
  private final GaugeService gaugeService
  private final PipelineCache pipelineCache
  private final Map<String, KafkaConsumer<String, String>> consumers

  @Autowired
  KafkaPollingAgent(GaugeService gaugeService,
                    PipelineCache pipelineCache,
                    @Value('${kafka.pollingIntervalMs:30000') long intervalMs) {
    this.intervalMs = intervalMs
    this.gaugeService = gaugeService
    this.pipelineCache = pipelineCache

    // TODO (jcs) inject servers!
    def servers = []
    consumers = servers.collectEntries { server ->
      def config = [
        "bootstrap.servers": server.address,
        "group.id": "kafka-spinnaker",
        "key.deserializer": StringDeserializer.class.getName(),
        "value.deserializer": StringDeserializer.class.getName(),
      ]
      return [server.name, new KafkaConsumer<String, String>(config)]
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

      // Get pipelines that have a Kafka trigger
      def pipelines = pipelineCache.getPipelines().findAll { pipeline ->
        pipeline.triggers && pipeline.triggers.any { TRIGGER_TYPE.equalsIgnoreCase(it.type) }
      }

      // Maps Kafka servers to a map of topics to a set of pipelines.
      // In other words, a server has many topics, each topic has many pipelines.
      Map<String, Map<String, Set<Pipeline>>> kafkaToPipeline =  []
      pipelines.each { pipeline ->
        def server = pipeline.properties['kafkaServer']
        def topic = pipeline.properties['kafkaTopic']
        def entry = kafkaToPipeline.get(server, [:].withDefault { new HashSet<Pipeline>() })
        entry[topic].add(pipeline)
      }

      // TODO (jcs) subscribe
      subscribe(kafkaToPipeline)

      this.consumers.each { name, consumer ->
        List<ConsumerRecord<String, String>> records = consumer.poll(0)
        records.each { record ->
          def topic = record.topic()
          def message = record.value()
          kafkaToPipeline.get(name).get(topic).each { pipeline ->
            // Trigger pipeline
          }
        }
      }
    } catch (Exception e) {
      log.error("Exception occurred while polling Kafka", e)
    } finally {
      gaugeService.submit("kafkaPollingAgent.executionTimeMillis", (double) System.currentTimeMillis() - start)
    }
  }

  /**
   * Subscribe to all Kafka topics specified by pipelines.
   *
   * This is an idempotent method in that it will compare the current state against the needed state.
   * Then it will resubcribe to all Kafka topics if there is a difference.
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
}

