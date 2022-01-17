package de.limago;

import JavaSessionize.avro.LogLine;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;

import java.util.Date;
import java.util.Map;
import java.util.Random;

public class SimpleKafkaProducer {

    private static final String TOPIC = "SimpleAvroNeuTopic";


    public void run() {

        try {
            var producer = createKafkaProducer();


            for (var i = 0; i < 5; i ++) {
                var logLine = createLogLine();
                var record = createProducerRecord(TOPIC, logLine.getIp().toString(),logLine);
                producer.send(record).get();
            }
        } catch (Exception e) {
            e.printStackTrace();
        }
    }

    private ProducerRecord<String, LogLine> createProducerRecord(String topic, String key, LogLine value) {
        return new ProducerRecord<>(topic, key, value);
    }

    private Producer<String, LogLine> createKafkaProducer() {
        return new KafkaProducer<>(createConfigMap());
    }


    private Map createConfigMap() {
        return  Map.of(
                "bootstrap.servers", "localhost:9092",
                "schema.registry.url", "http://localhost:8081",
                "acks", "all",
                "retries", 0,
                //"value.serializer", "io.confluent.kafka.serializers.json.KafkaJsonSchemaSerializer",
                "value.serializer", "io.confluent.kafka.serializers.KafkaAvroSerializer",

                "key.serializer", "org.apache.kafka.common.serialization.StringSerializer"
        );
    }

    private LogLine createLogLine() {
        Random r = new Random();
        LogLine logLine = new LogLine();
        int ip4 =r.nextInt(10);
        long runtime = new Date().getTime();
        logLine.setIp("66.249.1."+ ip4);
        logLine.setReferrer("www.example.com");
        logLine.setTimestamp(runtime);
        logLine.setUrl("http:// dingsbums.de");
        logLine.setUseragent("Mozilla/5.0 (Macintosh; Intel Mac OS X 10_10_4) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/44.0.2403.125 Safari/537.36");
        return logLine;
    }
}
