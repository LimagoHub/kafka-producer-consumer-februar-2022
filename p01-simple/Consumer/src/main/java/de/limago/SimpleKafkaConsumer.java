package de.limago;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static final String TOPIC = "SimpleTopic";

    public void run() {

        var consumer  = createKafkaConsumer();

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Reading topic:" + TOPIC);

        while (true) {
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofMillis(1000));


            for (ConsumerRecord<String, String> record: records) {
                String ip = record.key();
                String event = record.value();

                System.out.println(String.format("Key= '%s' Value = '%s'", ip, event));

            }
            consumer.commitSync();
        }

    }

    private KafkaConsumer<String, String> createKafkaConsumer() {
        return new KafkaConsumer<String, String>(createConfigMap());
    }

    private Properties createConfigMap() {
        var map =  Map.of(
                "bootstrap.servers", "localhost:9092",
                "value.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer",
                "group.id", "Dozent",
                "auto.commit.enable", "false",
                "auto.offset.reset", "earliest"
        );

        Properties props = new Properties();
        props.putAll(map);
        return props;
    }
}
