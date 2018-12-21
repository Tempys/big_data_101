/**
calculate the longest period of stay of couples with children
 */
select max(datediff(to_date(train.srch_co),to_date(train.srch_ci))) as dur  from train where train.srch_adults_cnt =2 and train.srch_children_cnt>0 ;