package com.shubham.kafka.consumer;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.LongDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.Collections;
import java.util.Properties;

public class KafkaConsumerExample {

    private String topic;

    private final static String TOPIC = "my-example-topic";

    private final static String BOOTSTRAP_SERVERS =
            "localhost:9092";

    private static Consumer<Long, String> createConsumer() {
        final Properties properties = new Properties();
        properties.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP_SERVERS);
        properties.put(ConsumerConfig.GROUP_ID_CONFIG, "group-1");
        properties.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, LongDeserializer.class.getName());
        properties.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        // Create the consumer using properties
        final Consumer<Long, String> consumer = new KafkaConsumer<>(properties);
        // Subscribe to the topic
        consumer.subscribe(Collections.singletonList(TOPIC));
        return consumer;
    }

    private static void runConsumer() {
        final Consumer<Long, String> consumer = createConsumer();
        final int giveUp = 100;
        int noRecordsCount = 0;
        while (true) {
            // The ConsumerRecords class is a container that holds a list of ConsumerRecord(s) per partition for a particular topic
            final ConsumerRecords<Long, String> consumerRecords = consumer.poll(1000);
            if (consumerRecords.count() == 0) {
                noRecordsCount++;
                if (noRecordsCount > giveUp) {
                    break;
                }
                continue;
            }
            consumerRecords.forEach(record -> {
                System.out.println("==== Consumer Record Key:" +
                        record.key() + " === Consumer Record Key:" + record.value() + "==Record Partition:" +
                        record.partition() + "=== Record Offset:" + record.offset());

            });
            consumer.commitAsync();
        }
        consumer.close();
        System.out.println("Done");
    }

    public static void main(String[] args) {
        System.out.println("Start Consumer");
        runConsumer();
    }

}
