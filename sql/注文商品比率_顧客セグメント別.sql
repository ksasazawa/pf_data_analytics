select
    a.item_name
    ,a.cnt_all
    ,b.cnt_first
    ,safe_divide(b.cnt_first, a.cnt_all) as item_first_rate
    ,c.cnt_repeat
    ,safe_divide(c.cnt_repeat, a.cnt_all) as item_repat_rate
    ,d.cnt_royal
    ,safe_divide(d.cnt_royal, a.cnt_all) as item_royal_rate
    ,e.cnt_break
    ,safe_divide(e.cnt_break, a.cnt_all) as item_break_rate
from
    /* 商品ごとの注文数 */
    (
        select
            item_name
            ,count(*) as cnt_all
        from
            `cafe-prd-dmt-387402.sales.customer_visits`
        group by
            item_name
    ) a
left join
    /* 初回来店顧客の商品ごとの注文数 */
    (
        select
            a.item_name
            ,count(*) as cnt_first
        from
            `cafe-prd-dmt-387402.sales.customer_visits` a
        inner join
            base b
        on
            date_trunc(a.day, month) = b.target_month
            and a.customer_id = b.customer_id
        where
            b.first_flg = 1
        group by
            item_name
    ) b
on
    a.item_name = b.item_name
left join
    /* リピート顧客の商品ごとの注文数 */
    (
        select
            a.item_name
            ,count(*) as cnt_repeat
        from
            `cafe-prd-dmt-387402.sales.customer_visits` a
        inner join
            base b
        on
            date_trunc(a.day, month) = b.target_month
            and a.customer_id = b.customer_id
        where
            b.repeat_flg = 1
        group by
            item_name
    ) c
on
    a.item_name = c.item_name
left join
    /* ロイヤル顧客の商品ごとの注文数 */
    (
        select
            a.item_name
            ,count(*) as cnt_royal
        from
            `cafe-prd-dmt-387402.sales.customer_visits` a
        inner join
            base b
        on
            date_trunc(a.day, month) = b.target_month
            and a.customer_id = b.customer_id
        where
            b.royal_flg = 1
        group by
            item_name
    ) d
on
    a.item_name = d.item_name
left join
    /* 離脱顧客の商品ごとの注文数 */
    (
        select
            a.item_name
            ,count(*) as cnt_break
        from
            `cafe-prd-dmt-387402.sales.customer_visits` a
        inner join
            base b
        on
            date_trunc(a.day, month) = b.target_month
            and a.customer_id = b.customer_id
        where
            b.break_flg = 1
        group by
            item_name
    ) e
on
    a.item_name = e.item_name
;
