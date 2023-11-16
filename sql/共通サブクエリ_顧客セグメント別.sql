/* 顧客毎の月間総支払額（月✖️顧客の全パターン） */
with customer_sales as (
    select
        a.target_month
        ,a.customer_id
        ,b.total_sales
        ,max(a.target_month)over(partition by null) as max_target_month /* 最新月 */
        ,min(case when b.total_sales is not null then a.target_month else null end)over(partition by a.customer_id order by a.target_month) as min_customer_target_month /* 顧客ごとの初回来店月 */
    from
        /* 月✖️顧客の全パターン */
        (
            select
                target_month
                ,customer_id
            from
                (
                    select
                        distinct date_trunc(day, month) as target_month
                    from
                        `cafe-prd-dmt-387402.sales.customer_visits`
                )
            cross join
                (
                    select
                        distinct customer_id
                    from
                        `cafe-prd-dmt-387402.sales.customer_visits`
                )
        ) a
    left join
        /* 顧客毎の月間総支払額 */
        (
            select
                date_trunc(day, month) as target_month
                ,customer_id
                ,sum(price) as total_sales
            from
                `cafe-prd-dmt-387402.sales.customer_visits`
            group by
                target_month
                ,customer_id
        ) b
    on
        a.target_month = b.target_month
        and a.customer_id = b.customer_id
)

/* 顧客毎の月間総支払額の月毎の平均 */
, avg_sales as (
    select
        target_month
        ,avg(total_sales) avg_sales
    from
        (
            select
                date_trunc(day, month) as target_month
                ,customer_id
                ,sum(price) as total_sales
            from
                `cafe-prd-dmt-387402.sales.customer_visits`
            group by
                target_month
                ,customer_id
        )
    group by
        target_month
)

/* 初回来店フラグ、ロイヤル顧客フラグ、離脱フラグを追加 */
, pre_base as (
    select
        target_month
        ,min_customer_target_month
        ,customer_id
        ,total_sales
        ,case when target_month = min_customer_target_month then 1 else 0 end as first_flg /* 初回来店フラグ（当月初めての来店） */
        ,case when total_sales > avg_sales
            and lag1_total_sales > lag1_avg_sales
            and lag2_total_sales > lag2_avg_sales
            and (lead1_total_sales is not null or target_month = max_target_month)
            then 1 else 0
        end as royal_flg /* ロイヤル顧客フラグ（直近３ヶ月間において月間総支払額がその月の平均を上回っており、かつ離脱でない） */
        ,case when total_sales > 0
            and target_month > min_customer_target_month
            and lead1_total_sales is null
            and target_month != max_target_month
            then 1 else 0
        end as break_flg /* 離脱フラグ（当月来店があり、次月来店なし） */
    from
        (
            select
                a.*
                ,lag(a.target_month)over(partition by a.customer_id order by a.target_month) as lag1_target_month /* 先月 */
                ,lag(a.target_month, 2)over(partition by a.customer_id order by a.target_month) as lag2_target_month /* 先々月 */
                ,lead(a.target_month)over(partition by a.customer_id order by a.target_month) as lead1_target_month /* 次月 */
                ,lag(a.total_sales)over(partition by a.customer_id order by a.target_month) as lag1_total_sales /* 先月の顧客毎支払い合計 */
                ,lag(a.total_sales, 2)over(partition by a.customer_id order by a.target_month) as lag2_total_sales /* 先々月の顧客毎支払い合計 */
                ,lead(a.total_sales)over(partition by a.customer_id order by a.target_month) as lead1_total_sales /* 次月の顧客毎支払い合計 */
                ,lag(b.avg_sales)over(partition by a.customer_id order by a.target_month) as lag1_avg_sales /* 先月の顧客毎支払い合計の平均 */
                ,lag(b.avg_sales, 2)over(partition by a.customer_id order by a.target_month) as lag2_avg_sales /* 先々月の顧客毎支払い合計の平均 */
                ,b.avg_sales
            from
                customer_sales a
            left join
                avg_sales b
            on
                a.target_month = b.target_month
        )
)

/* リピートフラグを追加 */
,base as (
    select
        *
        ,case when total_sales > 0
            and target_month > min_customer_target_month
            and royal_flg = 0
            and break_flg = 0
            then 1 else 0
        end as repeat_flg /* リピートフラグ（過去月に一度でも来店があり、かつロイヤルでも離脱でもない） */
    from
        pre_base
)
