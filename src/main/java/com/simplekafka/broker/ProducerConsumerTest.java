package com.simplekafka.broker;

import com.simplekafka.client.SimpleKafkaConsumer;
import com.simplekafka.client.SimpleKafkaProducer;

import java.nio.charset.StandardCharsets;

public class ProducerConsumerTest {

    public static void main(String[] args) throws Exception {
        SimpleKafkaProducer producer =
                new SimpleKafkaProducer("localhost", 9092, "test");

        producer.initialize();

        for (int i = 0; i < 5; i++) {
            long offset = producer.send("msg-" + i);
            System.out.println("Produced at offset: " + offset);
        }

        SimpleKafkaConsumer consumer =
                new SimpleKafkaConsumer("localhost", 9092, "test", 0);

        consumer.initialize();

        consumer.startConsuming((message, offset) -> {
            System.out.println("Consumed offset " + offset + ": " + new String(message, StandardCharsets.UTF_8));
        });

        Thread.sleep(5000);
        consumer.stop();
    }
}