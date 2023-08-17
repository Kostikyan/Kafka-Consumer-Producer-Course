package io.conductor.demos.kafka;

import org.apache.kafka.clients.producer.Callback;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Properties;

public class ProducerDemoWithCallback {
    private static final Logger log = LoggerFactory.getLogger(ProducerDemoWithCallback.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Im a Kafka producer");

        // create a producer properties
        Properties properties = new Properties();

        // connect to localhost
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set key and value serializer properties
        properties.setProperty("key.serializer", StringSerializer.class.getName());
        properties.setProperty("value.serializer", StringSerializer.class.getName());

        // set batch size to 400
        properties.setProperty("batch.size", "400");

        // create a producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(properties);
        for (int j = 0; j < 10; j++) {
            for (int i = 0; i < 30; i++) {
                // create a producer record
                ProducerRecord<String, String> producerRecord = new ProducerRecord<>("demo_java", "hello world " + i);

                // send data
                producer.send(producerRecord, (recordMetadata, e) -> {
                    // executes every time a record successfully sent or an exception is thrown
                    if(e == null){
                        // the record was successfully sent
                        log.info("Received new metadata \nTopic: {} \nPartition: {} \nOffset: {} \nTimestamp: {}", recordMetadata.topic(), recordMetadata.partition(), recordMetadata.offset(), recordMetadata.timestamp());
                    }else{
                        log.error("Error while producing", e);
                    }
                });
            }
            try {
                Thread.sleep(500);
            } catch (InterruptedException e) {
                throw new RuntimeException(e);
            }
        }

        // flush and close producer
        producer.flush();
        producer.close();
    }

}
