package de.limago;

import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class SimpleKafkaJsonProducer {

    private static final String TOPIC = "SimpleJsonTopic";


    public void run() {

        try {
            var producer = createKafkaProducer();


            for (var i = 0; i < 5; i ++) {
                var user = new User();
                var record = createProducerRecord(TOPIC, user.firstName + " " + i,user);
                producer.send(record).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ProducerRecord<String, User> createProducerRecord(String topic, String key, User value) {
        return new ProducerRecord<String, User>(topic, key, value);
    }

    private Producer<String, User> createKafkaProducer() {
        return new KafkaProducer<>(createConfigMap());
    }


    private Map createConfigMap() {
        System.setProperty("spring.kafka.consumer.properties.spring.json.trusted.packages","de.limago");
        return  Map.of(
                "bootstrap.servers", "localhost:9092",
                //"schema.registry.url", "http://localhost:8081",
                "acks", "all",
                "retries", 0,
                "value.serializer", "org.springframework.kafka.support.serializer.JsonSerializer",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        );

    }


}
