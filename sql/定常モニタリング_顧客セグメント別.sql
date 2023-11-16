select
    target_month
    ,count(case when total_sales > 0 then customer_id else null end) as all_cnt
    ,count(case when first_flg = 1 then 1 else null end) as first_cnt
    ,count(case when repeat_flg = 1 then 1 else null end) as repeat_cnt
    ,count(case when royal_flg = 1 then 1 else null end) as royal_cnt
    ,count(case when break_flg = 1 then 1 else null end) as break_cnt
    ,safe_divide(count(case when first_flg = 1 then 1 else null end), count(case when total_sales > 0 then customer_id else null end)) as first_rate
    ,safe_divide(count(case when repeat_flg = 1 then 1 else null end), count(case when total_sales > 0 then customer_id else null end)) as repeat_rate
    ,safe_divide(count(case when royal_flg = 1 then 1 else null end), count(case when total_sales > 0 then customer_id else null end)) as royal_rate
    ,safe_divide(count(case when break_flg = 1 then 1 else null end), count(case when total_sales > 0 then customer_id else null end)) as break_rate
from
    base
group by
    target_month
order by
    target_month
;
