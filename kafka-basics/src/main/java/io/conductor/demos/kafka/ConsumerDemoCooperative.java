package io.conductor.demos.kafka;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.CooperativeStickyAssignor;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Arrays;
import java.util.Properties;

public class ConsumerDemoCooperative {
    private static final Logger log = LoggerFactory.getLogger(ConsumerDemoCooperative.class.getSimpleName());

    public static void main(String[] args) {
        log.info("Consumer is started!");

        String groupId = "my-java-application";
        String topic = "demo_java";

        Properties properties = new Properties();
        properties.setProperty("bootstrap.servers", "127.0.0.1:9092");

        // set producer properties
        properties.setProperty("key.deserializer", StringDeserializer.class.getName());
        properties.setProperty("value.deserializer", StringDeserializer.class.getName());

        properties.setProperty("group.id", groupId);
        properties.setProperty("auto.offset.reset", "earliest");
        properties.setProperty("partition.assignment.strategy", CooperativeStickyAssignor.class.getName());


        KafkaConsumer<String, String> kafkaConsumer = new KafkaConsumer<>(properties);

        final Thread current = Thread.currentThread();

        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            log.info("detected a shutdown, let's exit by calling consumer.wakeup()....");
            kafkaConsumer.wakeup();

            try {
                current.join();
            } catch (InterruptedException e) {
                e.printStackTrace();
            }
        }));

        try {
            kafkaConsumer.subscribe(Arrays.asList(topic));

            while (true) {
                log.info("Polling");

                ConsumerRecords<String, String> poll = kafkaConsumer.poll(Duration.ofMillis(1000));

                for (ConsumerRecord<String, String> record : poll) {
                    log.info("Key: " + record.key(), "Value: " + record.value());
                    log.info("Partition: " + record.partition(), "Offset: " + record.offset());
                }
            }

        } catch (WakeupException e){
            log.info("Consumer is starting to shut down");
        } catch (Exception e){
            log.error("Unexpected exception in the consumer");
        } finally {
            kafkaConsumer.close();
            log.info("The consumer is now gracefully shut down");
        }
    }
}
