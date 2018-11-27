select train.hotel_continent,train.hotel_country,train.hotel_market,count(train.user_id) as num
from train
where train.is_booking	= 0
group by train.hotel_continent,train.hotel_country,train.hotel_market
ORDER BY num DESC LIMIT 3;