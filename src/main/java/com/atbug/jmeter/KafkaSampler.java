/*
 * Copyright 2014 Signal.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.atbug.jmeter;

import org.apache.jmeter.config.Arguments;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerClient;
import org.apache.jmeter.protocol.java.sampler.JavaSamplerContext;
import org.apache.jmeter.samplers.SampleResult;
import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.Future;

public class KafkaSampler implements JavaSamplerClient {

    private static final String PARAMETER_KAFKA_BROKERS = "kafka_brokers";
    private static final String PARAMETER_KAFKA_TOPIC = "kafka_topic";
    private static final String PARAMETER_KAFKA_KEY_SIZE = "kafka_key_size";
    private static final String PARAMETER_KAFKA_MESSAGE_SIZE = "kafka_message_size";
    private static final String PARAMETER_KAFKA_MESSAGE_SERIALIZER = "kafka_message_serializer";
    private static final String PARAMETER_KAFKA_KEY_SERIALIZER = "kafka_key_serializer";
    private static final String PARAMETER_KAFKA_ACK_MODE = "kafka_ack_mode";
    private static final String PARAMETER_KAFKA_LINGER_MS = "kafka_linger_ms";
    private static final String PARAMETER_KAFKA_BUFFER_MEMORY_INT = "kafka_buffer_memory_int";
    public static final String PARAMETER_KAFKA_BATCH_SIZE_INT = "kafka_batch_size_int";
    public static final String PARAMETER_KAFKA_MAX_REQUEST_SIZE_INT = "kafka_max_request_size_int";
    public static final String PARAMETER_PRODUCER_ACK = "kafka_producer_ack";
    public static final String PARAMETER_PRODUCER_CALLBACK = "kafka_producer_callback";
    private KafkaProducer<byte[], byte[]> producer;

    private boolean isAck = false;
    private boolean isCallback = false;
    private byte[] key = null;
    private byte[] payload = null;

    @Override
    public void setupTest(JavaSamplerContext context) {
        Properties config = new Properties();
        config.put("bootstrap.servers", context.getParameter(PARAMETER_KAFKA_BROKERS));
        config.put("key.serializer", context.getParameter(PARAMETER_KAFKA_KEY_SERIALIZER));
        config.put("value.serializer", context.getParameter(PARAMETER_KAFKA_MESSAGE_SERIALIZER));
        if (context.getParameter(PARAMETER_KAFKA_LINGER_MS) != null && context.getParameter
                (PARAMETER_KAFKA_LINGER_MS).length() > 0)
            config.put(ProducerConfig.LINGER_MS_CONFIG, context.getIntParameter(PARAMETER_KAFKA_LINGER_MS));
        if (context.getParameter(PARAMETER_KAFKA_ACK_MODE) != null && context.getParameter(PARAMETER_KAFKA_ACK_MODE)
                .length() > 0)
            config.put(ProducerConfig.ACKS_CONFIG, context.getParameter(PARAMETER_KAFKA_ACK_MODE));
        if (context.getParameter(PARAMETER_KAFKA_BUFFER_MEMORY_INT) != null && context.getParameter
                (PARAMETER_KAFKA_BUFFER_MEMORY_INT).length() > 0) {
            config.put(ProducerConfig.BUFFER_MEMORY_CONFIG, context.getIntParameter(PARAMETER_KAFKA_BUFFER_MEMORY_INT));
        }
        if (context.getParameter(PARAMETER_KAFKA_BATCH_SIZE_INT) != null && context.getParameter
                (PARAMETER_KAFKA_BATCH_SIZE_INT).length() > 0) {
            config.put(ProducerConfig.BATCH_SIZE_CONFIG, context.getIntParameter(PARAMETER_KAFKA_BATCH_SIZE_INT));
        }
        if (context.getParameter(PARAMETER_KAFKA_MAX_REQUEST_SIZE_INT) != null && context.getParameter
                (PARAMETER_KAFKA_MAX_REQUEST_SIZE_INT).length() > 0) {
            config.put(ProducerConfig.MAX_REQUEST_SIZE_CONFIG,
                    context.getIntParameter(PARAMETER_KAFKA_MAX_REQUEST_SIZE_INT));
        }
        if (context.getParameter(PARAMETER_PRODUCER_ACK) != null && "true"
                .equals(context.getParameter(PARAMETER_PRODUCER_ACK))) {
            isAck = true;
        }
        if (context.getParameter(PARAMETER_PRODUCER_CALLBACK) != null && "true"
                .equals(context.getParameter(PARAMETER_PRODUCER_CALLBACK))) {
            isCallback = true;
        }
        producer = new KafkaProducer<>(config);

        key = new byte[context.getIntParameter(PARAMETER_KAFKA_KEY_SIZE)];
        payload = new byte[context.getIntParameter(PARAMETER_KAFKA_MESSAGE_SIZE)];
        Arrays.fill(key, (byte) 1);
        Arrays.fill(payload, (byte) 1);
    }

    @Override
    public void teardownTest(JavaSamplerContext context) {
        producer.close();
    }

    @Override
    public Arguments getDefaultParameters() {
        Arguments defaultParameters = new Arguments();
        defaultParameters.addArgument(PARAMETER_KAFKA_BROKERS, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_TOPIC, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY_SIZE, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_MESSAGE_SIZE, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_ACK_MODE, "");
        defaultParameters.addArgument(PARAMETER_PRODUCER_CALLBACK, "");
        defaultParameters.addArgument(PARAMETER_PRODUCER_ACK, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_LINGER_MS, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_BUFFER_MEMORY_INT, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_BATCH_SIZE_INT, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_MAX_REQUEST_SIZE_INT, "");
        defaultParameters.addArgument(PARAMETER_KAFKA_MESSAGE_SERIALIZER,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        defaultParameters.addArgument(PARAMETER_KAFKA_KEY_SERIALIZER,
                "org.apache.kafka.common.serialization.ByteArraySerializer");
        return defaultParameters;
    }

    @Override
    public SampleResult runTest(JavaSamplerContext context) {
        SampleResult result = newSampleResult();

        String topic = context.getParameter(PARAMETER_KAFKA_TOPIC);

        result.sampleStart();
        try {
            Future<RecordMetadata> future;
            if (isCallback && !isAck) {
                future = producer.send(
                        new ProducerRecord<>(topic, key, payload), new Callback() {
                            @Override
                            public void onCompletion(RecordMetadata metadata, Exception exception) {
                                if (exception != null) {
                                    exception.printStackTrace();
                                }
                            }
                        });
            } else {
                future = producer.send(new ProducerRecord<>(topic, key, payload));
            }
            if (isAck && !isCallback) {
                future.get();
            }
            result.setSuccessful(true);
        } catch (Exception e) {
            result.setSuccessful(false);
        }
        result.sampleEnd();
        return result;
    }

    /**
     * Factory for creating new {@link SampleResult}s.
     */
    private SampleResult newSampleResult() {
        SampleResult result = new SampleResult();
        return result;
    }
}
