package de.limago;

import JavaSessionize.avro.LogLine;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.specific.SpecificData;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static final String TOPIC = "SimpleAvroNeuTopic";

    public void run() {

        var consumer  = createKafkaConsumer();

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Reading topic:" + TOPIC);

        while (true) {
            var records = consumer.poll(Duration.ofMillis(1000));


            for (var record: records) {
                String key = record.key();
                GenericRecord message = record.value();
                var logLine = (LogLine) SpecificData.get().deepCopy(LogLine.SCHEMA$, message);

                System.out.println(String.format("Key= '%s' Value = '%s'", key, logLine));

            }
            consumer.commitSync();
        }

    }

    private KafkaConsumer<String, LogLine> createKafkaConsumer() {
        return new KafkaConsumer<String, LogLine>(createConfigMap());
    }

    private Properties createConfigMap() {
        var map =  Map.of(
                "bootstrap.servers", "localhost:9092",
                "schema.registry.url", "http://localhost:8081",
                "value.deserializer", "io.confluent.kafka.serializers.KafkaAvroDeserializer",
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
