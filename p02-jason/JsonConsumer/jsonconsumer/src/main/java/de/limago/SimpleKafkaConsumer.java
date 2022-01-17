package de.limago;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.springframework.kafka.support.serializer.JsonDeserializer;

import java.time.Duration;
import java.util.Collections;
import java.util.Map;
import java.util.Properties;

public class SimpleKafkaConsumer {
    private static final String TOPIC = "SimpleJsonTopic";

    public void run() {

        var consumer  = createKafkaConsumer();

        consumer.subscribe(Collections.singletonList(TOPIC));

        System.out.println("Reading topic:" + TOPIC);

        while (true) {
            var records = consumer.poll(Duration.ofMillis(1000));


            for (var record: records) {
                var key = record.key();
                var user = record.value();

                System.out.println(String.format("Key= '%s' Value = '%s'", key, user));

            }
            consumer.commitSync();
        }

    }

    private KafkaConsumer<String, User> createKafkaConsumer() {
        return new KafkaConsumer<String, User>(createConfigMap());
    }

    private Properties createConfigMap() {
        System.setProperty("spring.kafka.consumer.properties.spring.json.trusted.packages","de.limago");
        var map =  Map.of(
                "bootstrap.servers", "localhost:9092",
                "value.deserializer", "org.springframework.kafka.support.serializer.JsonDeserializer",
                "key.deserializer", "org.apache.kafka.common.serialization.StringDeserializer"  ,
                "group.id", "Dozent",
                "auto.commit.enable", "false",
                "auto.offset.reset", "earliest",
                JsonDeserializer.TRUSTED_PACKAGES, "*"
        );

        Properties props = new Properties();
        props.putAll(map);
        return props;
    }


}
