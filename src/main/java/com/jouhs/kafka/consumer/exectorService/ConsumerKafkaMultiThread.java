package com.jouhs.kafka.consumer.exectorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;

import java.lang.reflect.Array;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

public class ConsumerKafkaMultiThread {
    public static void main(String[] args) {
        Properties props = new Properties();
        props.put("bootstrap.servers", "localhost:9092");
        props.put("group.id", "test-group-id");
        props.put("enable.auto.commit", "true");
        props.put("auto.commit.interval.ms","1000");
        props.put("key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");
        props.put("value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer");

        BasicConsumeLoop basicConsumeLoop1 = new BasicConsumeLoop(props, Arrays.asList("testMultiThread")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumer 1 => offset= %d, key = %s, value = %s %n", record.offset(), record.key(), record.value());
            }
        };

        BasicConsumeLoop basicConsumeLoop2 = new BasicConsumeLoop(props, Arrays.asList("testMultiThread")) {
            @Override
            public void process(ConsumerRecord record) {
                System.out.printf("Consumer 2 => offset= %d, key = %s, value = %s %n", record.offset(), record.key(), record.value());
            }
        };

        ExecutorService executorService = Executors.newFixedThreadPool(2);
        executorService.execute(basicConsumeLoop1);
        executorService.execute(basicConsumeLoop2);
    }
}
