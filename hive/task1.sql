/**
calculate Top 3 most popular countries where booking is successful (booking = 1)
 */
select train.hotel_country,count(train.user_id) as user_count from train
where train.is_booking = 1
group by train.hotel_country
ORDER BY user_count DESC LIMIT 3;