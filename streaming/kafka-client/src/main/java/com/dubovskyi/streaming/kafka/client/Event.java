package com.dubovskyi.streaming.kafka.client;


import lombok.Data;

/***
 * The structure of event which we will send to kafka
 */
@Data
public class Event {
   private int hotel_cluster;
    private long cnt;
    private int is_booking;
    private int hotel_market;
    private int hotel_country;
    private int hotel_continent;
    private int srch_destination_type_id;
    private int srch_rm_cnt;
    private int srch_children_cnt;
    private int srch_adults_cnt;
    private String srch_co;
    private String srch_ci;
    private int channel;
    private int is_package;
    private int is_mobile;
    private int user_id;
    private double orig_destination_distance;
    private  int user_location_city;
    private int user_location_region;
    private int user_location_country;
    private int posa_continent;
    private int site_name;
    private String date_time;
}
