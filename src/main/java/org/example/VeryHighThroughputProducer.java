package org.example;

import org.apache.kafka.clients.producer.*;
import org.apache.kafka.common.serialization.StringSerializer;
import org.json.JSONObject;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.time.Duration;
import java.util.Properties;
import java.util.Random;
import java.util.concurrent.atomic.AtomicBoolean;


public class VeryHighThroughputProducer {

    private static final String KAFKA_BROKER = "kafka:29092";
    private static final String TOPIC_NAME = "very_high_throughput_topic";
    private static final AtomicBoolean running = new AtomicBoolean(true);
    private static final Logger log = LoggerFactory.getLogger(VeryHighThroughputProducer.class);

    public static void main(String[] args) {
        // Set up producer properties
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, KAFKA_BROKER);
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        // Configure for higher throughput, mimicking the Python script
        props.put(ProducerConfig.ACKS_CONFIG, "all");
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, 16384);
        props.put(ProducerConfig.COMPRESSION_TYPE_CONFIG, "gzip");
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, 33554432L);

        // Create the Kafka producer
        KafkaProducer<String, String> producer = new KafkaProducer<>(props);

        log.info("Starting Kafka Producer for topic: " + TOPIC_NAME);
        // Add a shutdown hook to handle graceful shutdown
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            System.out.println("Stopping producer...");
            running.set(false);
            // The close() method with a boolean argument is for transactions, which isn't used here.
            // The correct method to call is close(Duration).
            producer.close(Duration.ofSeconds(10));
        }));

        try {
            log.info("generating messages");
            while (running.get()) {
                String message = generateMessage();
                producer.send(new ProducerRecord<>(TOPIC_NAME, message), new Callback() {
                    @Override
                    public void onCompletion(RecordMetadata metadata, Exception exception) {
                        if (exception != null) {
                            exception.printStackTrace();
                        }
                    }
                });
            }
        } catch (Exception e) {
            e.printStackTrace();
        } finally {
            // The close() method without arguments is deprecated.
            // A graceful shutdown should use a timeout.
            producer.close(Duration.ofSeconds(10));
        }
    }

    private static String generateMessage() {
        Random random = new Random();
        JSONObject data = new JSONObject();
        data.put("user_id", 1000 + random.nextInt(9000));
        String[] eventTypes = {"click", "purchase", "view"};
        data.put("event_type", eventTypes[random.nextInt(eventTypes.length)]);
        data.put("timestamp", System.currentTimeMillis());
        data.put("value", 1.0 + random.nextDouble() * 99.0);
        return data.toString();
    }
}
