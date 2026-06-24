package com.wikipedia.intel;

import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.kafka.common.serialization.StringSerializer;

import java.time.Duration;
import java.util.List;
import java.util.Properties;

/**
 * Quick smoke test: produce a message, consume it back.
 * Run with: ./gradlew run -PmainClass=com.wikipedia.intel.KafkaVerify
 * Requires Docker Kafka to be running.
 */
public class KafkaVerify {

    private static final String BOOTSTRAP = "localhost:9092";
    private static final String TOPIC = "wikipedia.test";

    public static void main(String[] args) throws Exception {
        // Create topic
        Properties adminProps = new Properties();
        adminProps.put("bootstrap.servers", BOOTSTRAP);
        try (AdminClient admin = AdminClient.create(adminProps)) {
            admin.createTopics(List.of(new NewTopic(TOPIC, 1, (short) 1))).all().get();
            System.out.println("✓ Topic created: " + TOPIC);
        } catch (Exception e) {
            System.out.println("→ Topic may already exist: " + e.getMessage());
        }

        // Produce
        Properties prodProps = new Properties();
        prodProps.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        prodProps.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        prodProps.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        try (KafkaProducer<String, String> producer = new KafkaProducer<>(prodProps)) {
            producer.send(new ProducerRecord<>(TOPIC, "test-key", "hello from wikipedia-intel")).get();
            System.out.println("✓ Message produced");
        }

        // Consume
        Properties consProps = new Properties();
        consProps.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, BOOTSTRAP);
        consProps.put(ConsumerConfig.GROUP_ID_CONFIG, "wikipedia-intel-verify");
        consProps.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class.getName());
        consProps.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");

        try (KafkaConsumer<String, String> consumer = new KafkaConsumer<>(consProps)) {
            consumer.subscribe(List.of(TOPIC));
            ConsumerRecords<String, String> records = consumer.poll(Duration.ofSeconds(10));
            if (records.isEmpty()) {
                System.out.println("✗ No messages received");
                System.exit(1);
            }
            records.forEach(r -> System.out.println("✓ Consumed: " + r.value()));
        }

        System.out.println("\n✓ Kafka connectivity verified!");
    }
}
