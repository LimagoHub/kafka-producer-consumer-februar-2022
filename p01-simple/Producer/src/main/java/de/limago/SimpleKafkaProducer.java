package de.limago;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Map;

public class SimpleKafkaProducer {

    private static final String TOPIC = "SimpleTopic";


    public void run() {

        try {
            var producer = createKafkaProducer();


            for (var i = 0; i < 5; i ++) {
                var record = createProducerRecord(TOPIC, "Hallo " + i,"Hallo " + i);
                producer.send(record).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ProducerRecord<String, String> createProducerRecord(String topic, String key, String value) {
        return new ProducerRecord<>(topic, key, value);
    }

    private Producer<String, String> createKafkaProducer() {
        return new KafkaProducer<>(createConfigMap());
    }


    private Map createConfigMap() {
        return  Map.of(
                "bootstrap.servers", "localhost:9092",
                //"schema.registry.url", "http://localhost:8081",
                "acks", "all",
                "retries", 0,
                "value.serializer", "org.apache.kafka.common.serialization.StringSerializer",
                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        );
    }
}
