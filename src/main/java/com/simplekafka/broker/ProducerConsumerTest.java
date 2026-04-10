package com.simplekafka.broker;

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

        consumer.startConsuming(message -> {
            System.out.println("Consumed: " + message);
        });
    }
}