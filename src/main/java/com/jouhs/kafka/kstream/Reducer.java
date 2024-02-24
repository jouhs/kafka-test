package com.jouhs.kafka.kstream;

import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.Materialized;

import java.io.FileInputStream;
import java.io.IOException;
import java.util.Properties;

public class Reducer {
    public static void main(String[] args) throws IOException {
        Properties props = new Properties();
        try(FileInputStream fis = new FileInputStream("src/main/resources/config/kstream.properties")) {
            props.load(fis);
        }
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "basic-streams-1");
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_DOC,0);

        final StreamsBuilder builder = new StreamsBuilder();
        KStream<String, String> streamPurchase = builder.stream("input-purchases");

        streamPurchase
                .groupByKey()
                .reduce((a,b) -> String.valueOf(Integer.valueOf(a)+Integer.valueOf(b)), Materialized.with(Serdes.String(), Serdes.String()))
                .toStream()
                .to("total-purchases");

        KafkaStreams kafkaStreams = new KafkaStreams(builder.build(), props);

        kafkaStreams.start();
    }
}
