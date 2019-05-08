package com.neeraj.kafka;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.Topology;
import org.apache.kafka.streams.kstream.KStream;

import java.util.Properties;
import java.util.concurrent.CountDownLatch;

/**
 * Simple Stream of Pipe example in which kafka-streams read from streams-plaintext-input topic and publish to streams-pipe-output
 * <p>
 * To Run this program, just make sure to run zookeeper, kafka cluster at 9092 port
 * <p>
 * and Run these 2 commands in 2 different terminal to see the complete program.
 * <p>
 * 1) bin/kafka-console-producer.sh --broker-list localhost:9092 --topic streams-plaintext-input
 * 2) bin/kafka-console-consumer.sh \
 * *      --bootstrap-server localhost:9092 \
 * *      --topic streams-pipe-output \
 * *      --from-beginning
 * *      --formatter kafka.tools.DefaultMessageFormatter \
 * *      --property print.value=true \
 * *      --property value.deserializer=org.apache.kafka.common.serialization.StringDeserializer
 *
 * @author neeraj on 2019-05-08
 * Copyright (c) 2019, kafka-playground.
 * All rights reserved.
 */
public class Pipe {

    public static void main(String[] args) {
        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-pipe");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");    // assuming that the Kafka broker this application is talking to runs on local machine with port 9092
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        final StreamsBuilder streamsBuilder = new StreamsBuilder();

        KStream<String, String> sourceStream = streamsBuilder.stream("streams-plaintext-input");
        sourceStream.to("streams-pipe-output");

        final Topology topology = streamsBuilder.build();

        System.out.println(topology.describe());

        final KafkaStreams kafkaStreams = new KafkaStreams(topology, props);

        final CountDownLatch countDownLatch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-shutdown-hook") {
            @Override
            public void run() {
                kafkaStreams.close();
                countDownLatch.countDown();
            }
        });

        try {
            kafkaStreams.start();
            countDownLatch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }
}
