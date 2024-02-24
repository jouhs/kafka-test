package com.jouhs.kafka.consumer.exectorService;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.util.List;
import java.util.Properties;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class BasicConsumeLoop<K, V> implements Runnable{
    private final KafkaConsumer<K, V> consumer;
    private final List<String> topics;
    private final AtomicBoolean shutdown;

    public BasicConsumeLoop(Properties config, List<String> topics){
        this.consumer = new KafkaConsumer<>(config);
        this.topics = topics;
        this.shutdown = new AtomicBoolean(false);
    }

    public abstract void process(ConsumerRecord<K, V> record);

    public void run(){
        try {
            consumer.subscribe(topics);
            while (!shutdown.get()){
                ConsumerRecords<K, V> records = consumer.poll(500);
                records.forEach(r -> process(r));
            }
        } finally {
            consumer.close();
        }
    }
}
