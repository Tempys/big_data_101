package com.dubovskyi.streaming.kafka.client;

import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.util.Properties;

import static org.junit.jupiter.api.Assertions.*;
import static org.junit.jupiter.api.Assertions.assertNotNull;

public class EventGeneratorTest {

    private EventGenerator eventGenerator = new EventGenerator(new Properties());

    @Test
    public void generateSingLeEvent(){
       Event event =  eventGenerator.generateEvent();

        assertNotNull(event.getChannel());
        assertNotNull(event.getHotel_cluster());
        assertEquals(0,event.getIs_booking());
    }
}
