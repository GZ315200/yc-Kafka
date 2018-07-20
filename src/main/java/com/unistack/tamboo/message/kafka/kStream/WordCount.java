package com.unistack.tamboo.message.kafka.kStream;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.common.serialization.Serde;
import org.apache.kafka.common.serialization.Serdes;
import org.apache.kafka.streams.Consumed;
import org.apache.kafka.streams.KafkaStreams;
import org.apache.kafka.streams.StreamsBuilder;
import org.apache.kafka.streams.StreamsConfig;
import org.apache.kafka.streams.kstream.KStream;
import org.apache.kafka.streams.kstream.KTable;
import org.apache.kafka.streams.kstream.Produced;

import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.CountDownLatch;

import static com.unistack.tamboo.message.kafka.util.CommonUtils.getSecurityProps;

/**
 * @author Gyges Zean
 * @date 2018/7/19
 */
public class WordCount {


    public static final String BOOTSTRAP_SERVERS = "192.168.1.110:9093,192.168.1.111:9093,192.168.1.112:9093";

    public static void main(String[] args) {


        Properties props = new Properties();
        props.put(StreamsConfig.APPLICATION_ID_CONFIG, "streams-wordcount-9");
        props.put(StreamsConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        props.put(StreamsConfig.CACHE_MAX_BYTES_BUFFERING_CONFIG, 0);
        props.put(StreamsConfig.DEFAULT_KEY_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.put(StreamsConfig.DEFAULT_VALUE_SERDE_CLASS_CONFIG, Serdes.String().getClass().getName());
        props.putAll(getSecurityProps(BOOTSTRAP_SERVERS));

        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");


        StreamsBuilder builder = new StreamsBuilder();


//        KStream<String, String> source = builder.stream("streams-plaintext-input");
//
//
//        KTable<String, Long> counts = source.flatMapValues((ValueMapper<String, Iterable<String>>) value ->
//                Arrays.asList(value.toLowerCase(Locale.getDefault()).split(",")))
//                .groupBy((key, value) -> value).count();


// Serializers/deserializers (serde) for String and Long types
        final Serde<String> stringSerde = Serdes.String();
        final Serde<Long> longSerde = Serdes.Long();



// Construct a `KStream` from the input topic "streams-plaintext-input", where message values
// represent lines of text (for the sake of this example, we ignore whatever may be stored
// in the message keys).
        KStream<String, String> textLines = builder.stream("streams-plaintext-input", Consumed.with(stringSerde, stringSerde));

        KTable<String, Long> wordCounts = textLines
                // Split each text line, by whitespace, into words.  The text lines are the message
                // values, i.e. we can ignore whatever data is in the message keys and thus invoke
                // `flatMapValues` instead of the more generic `flatMap`.
                .flatMapValues(value ->

                        Arrays.asList(value.toLowerCase().split("\\W+")))
                // We use `groupBy` to ensure the words are available as message keys
                .groupBy((key, value) -> value)
                // Count the occurrences of each word (message key).
                .count();

// Convert the `KTable<String, Long>` into a `KStream<String, Long>` and write to the output topic.
        wordCounts.toStream().to("streams-wordcount-output", Produced.with(stringSerde, longSerde));


//        counts.toStream().to("streams-wordcount-output", Produced.with(Serdes.String(), Serdes.Long()));


        final KafkaStreams streams = new KafkaStreams(builder.build(), props);
        final CountDownLatch latch = new CountDownLatch(1);

        // attach shutdown handler to catch control-c
        Runtime.getRuntime().addShutdownHook(new Thread("streams-wordcount-shutdown-hook") {
            @Override
            public void run() {
                streams.close();
                latch.countDown();
            }
        });

        try {
            streams.start();
            latch.await();
        } catch (Throwable e) {
            System.exit(1);
        }
        System.exit(0);
    }

}
