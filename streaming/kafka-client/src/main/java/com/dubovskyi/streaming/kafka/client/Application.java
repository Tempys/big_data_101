package com.dubovskyi.streaming.kafka.client;

import java.io.IOException;
import java.util.Properties;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

/**
 * Main class which start all logic
 */
public class Application {

    public static void main(String[] args) throws IOException {

        Properties properties =new Properties();
        properties.load(Application.class.getClassLoader().getResourceAsStream("application.properties"));
        int number = Integer.parseInt(properties.getProperty("thread.number"));
        long delay = Long.parseLong(properties.getProperty("delay"));

        final ExecutorService executorService = Executors.newFixedThreadPool(number);
        final  EventGenerator eventGenerator = new EventGenerator(properties);

        for(int i=0;i<number;i++ ){
            executorService.submit(() -> eventGenerator.sendToKafka(delay) );
        }

    }

}
