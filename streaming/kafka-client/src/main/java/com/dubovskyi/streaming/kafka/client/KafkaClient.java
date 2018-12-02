package com.dubovskyi.streaming.kafka.client;

import com.dubovskyi.streaming.kafka.client.domain.event.Event;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.clients.producer.RecordMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.serialization.ByteArraySerializer;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.kafka.connect.json.JsonSerializer;

import java.io.IOException;
import java.util.Arrays;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import static com.dubovskyi.streaming.kafka.client.AvroHelper.serialize;

public class KafkaClient {

    public static void main(String[] str) throws InterruptedException, IOException {
        System.out.println("Starting ProducerAvroExample ...");
        Event event = new Event();
        event.setId(1L);
        event.setIsBooking(0);
        sendMessages(event);
    }

    private static void sendMessages(Event event) throws InterruptedException, IOException {
        Producer<String, JsonNode> producer = createProducer();
        sendJsonRecords(producer,event);
    }

    private static Producer<String, JsonNode> createProducer() {
        Properties props = new Properties();
        props.put("bootstrap.servers", "192.168.99.100:9092");
        props.put("acks", "all");
       props.put("retries", 3);
      //  props.put("batch.size", 16384);
      props.put("linger.ms", 1);
      //  props.put("buffer.memory", 33554432);
        props.put("key.serializer", StringSerializer.class.getName());
        props.put("value.serializer", JsonSerializer.class.getName());
        return new KafkaProducer<String, JsonNode>(props);
    }


    private static void sendJsonRecords(Producer<String, JsonNode> producer,Event event){
        ObjectMapper objectMapper = new ObjectMapper();
        String topic = "streaming1";
        int partition = 0;

        TestPojo testPojo = new TestPojo();
        testPojo.setA("dd");
        testPojo.setB("ddef");

        JsonNode  jsonNode = objectMapper.valueToTree(testPojo);

        Future<RecordMetadata> result =  producer.send(new ProducerRecord<String, JsonNode>(topic, partition, Integer.toString(0), jsonNode));

        try {
            System.out.println(result.get().offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }
    }

    private static void sendRecords(Producer<String, byte[]> producer,Event event) throws IOException {
        String topic = "streaming";
        int partition = 0;

        Future<RecordMetadata> result =   producer.send(new ProducerRecord<String, byte[]>(topic, partition, Integer.toString(0), record(event)));

        try {
            System.out.println(result.get().offset());
        } catch (InterruptedException e) {
            e.printStackTrace();
        } catch (ExecutionException e) {
            e.printStackTrace();
        }


    }

    private static byte[] record(Event event) throws IOException {
        return serialize(event);
    }
}

