package com.dubovskyi.streaming.kafka.client;

import java.time.LocalDateTime;
import java.time.ZoneId;
import java.util.Properties;
import java.util.concurrent.ThreadLocalRandom;

/**
 * In this class automatically generate and sending data to kafka
 */
public class EventGenerator {

    private final KafkaClient kafkaClient;

    public EventGenerator(Properties properties) {
        this.kafkaClient = new KafkaClient(properties);
    }

    /**
     * Randomly generate event
     * @return
     */
    protected Event generateEvent(){
        Event event = new Event();
        int randomNum = ThreadLocalRandom.current().nextInt(0, 99999999 );

        event.setChannel(randomNum);
        event.setHotel_cluster(randomNum+3);
        event.setIs_booking(0);
        event.setDate_time(LocalDateTime.now().atZone(ZoneId.of("UTC")).toInstant().toEpochMilli());

        return event;

    }

    /**
     * class send data to kafka continiously
     * @param delay create delay before next sending event to kafka
     */
    public void sendToKafka(long delay){

      while (true) {

            Event event = generateEvent();
            kafkaClient.sendMessage(event);

            stop(delay);
        }
    }

    /**
     * create delay after sending to kafka
     * @param delay
     */
    private void stop(long delay){
      try {
            Thread.sleep(delay);
        } catch (InterruptedException e) {
            e.printStackTrace();
        }
    }

}
