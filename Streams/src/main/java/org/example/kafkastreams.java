package org.example;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KStreamBuilder;
import org.apache.kafka.streams.kstream.KTable;

import java.util.Arrays;
import java.util.Properties;


public class kafkastreams {

    public static void main(String[] args) {

        Properties config = new Properties();
        config.put(StreamsConfig.APPLICATION_ID_CONFIG, "kafka-streams");
        config.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, "localhost:9092");
        config.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest" );
        config.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass());
        config.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass());

        KStreamBuilder builder = new KStreamBuilder();

        // 1 - stream from Kafka
        KStream<String, String> wordcountinput = builder.stream( "word-count-input");

        KTable<String, Long> wordcounts = wordcountinput

                // 2 - map values to lowercase
                .mapValues(textline -> textline.toLowerCase())

                // 3 - flatmap values split by space
                .flatMapValues(lowercasetextline -> Arrays.asList(lowercasetextline.split(" ")))

                // 4 - select key to apply a key (we discard the old key)
                .selectKey((ignoredkey, word) -> word)

                // 5 - group by key before aggregation
                .groupByKey()

                // 6 - count occurrences
                .count( "counts");

        wordcounts.to(Serdes.String(), Serdes.Long(),"word-count-output");

        KafkaStreams streams = new KafkaStreams(builder, config);
        streams.start();

        //printed the topology
        System.out.println(streams.toString());

        // shutdown hook to correctly close the streams application
        Runtime.getRuntime().addShutdownHook(new Thread(streams::close));
    }

}
