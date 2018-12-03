package com.dubovskyi.streaming.kafka.client;


import lombok.Data;

@Data
public class Event {
    int hotel_cluster;
    long cnt;
    int is_booking;
    int hotel_market;
    int hotel_country;
    int hotel_continent;
    int srch_destination_type_id;
    int srch_rm_cnt;
    int srch_children_cnt;
    int srch_adults_cnt;
    String srch_co;
    String srch_ci;
    int channel;
    int is_package;
    int is_mobile;
    int user_id;
    double orig_destination_distance;
    int user_location_city;
    int user_location_region;
    int user_location_country;
    int posa_continent;
    int site_name;
    String date_time;
}
