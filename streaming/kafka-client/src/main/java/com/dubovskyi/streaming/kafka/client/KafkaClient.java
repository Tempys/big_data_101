package com.dubovskyi.streaming.kafka.client;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

/**
 * Class which integrates with Kafka
 * Data send to kafka in json
 */
public class KafkaClient {

    private final Properties properties;
    private String bootstrapServers;
    private String topic;

    public KafkaClient(Properties properties) {
        this.properties = properties;
        this.bootstrapServers =properties.getProperty("bootstrap.servers");
        this.topic = properties.getProperty("kafka.topic");
    }

    public  void sendMessage(Event event) {
        Producer<String, JsonNode> producer = createProducer();
        sendJsonRecords(producer,event);
    }

    private  Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", bootstrapServers);
        props.put("acks", "all");
        props.put("retries", 3);
        props.put("linger.ms", 1);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", JsonSerializer.class.getName());
        return new KafkaProducer<String, JsonNode>(props);
    }

    private  void sendJsonRecords(Producer<String, JsonNode> producer,Event event){
        ObjectMapper objectMapper = new ObjectMapper();
        int partition = 0;
        JsonNode  jsonNode = objectMapper.valueToTree(event);

        Future<RecordMetadata> result =  producer.send(new ProducerRecord<String, JsonNode>(topic, partition, Integer.toString(0), jsonNode));

        try {
            System.out.println(result.get().offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }


}

