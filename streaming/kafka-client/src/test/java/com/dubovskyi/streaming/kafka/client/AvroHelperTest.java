package com.dubovskyi.streaming.kafka.client;

import com.dubovskyi.streaming.kafka.client.domain.event.Event;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.Test;

import java.io.IOException;

import static org.junit.jupiter.api.Assertions.assertEquals;

public class AvroHelperTest {

    @Test
    public void singleEventMessage() throws IOException {
        Event event = EventGenerator.generateEvent(1L,33);
        byte[] array = AvroHelper.serialize(event);
        Event result = AvroHelper.deserialize(array);

        assertEquals(1L,result.getId().longValue());
        assertEquals(33,result.getIsBooking().intValue());

    }




}
