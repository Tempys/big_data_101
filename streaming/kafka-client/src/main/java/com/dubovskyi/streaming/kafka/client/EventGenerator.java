package com.dubovskyi.streaming.kafka.client;

import com.dubovskyi.streaming.kafka.client.domain.event.Event;

public class EventGenerator {

  public static   Event generateEvent(Long id,int booking){
        Event event = new Event();

        event.setId(id);
        event.setIsBooking(booking);



        return event;

    }
}
