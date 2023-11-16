create or replace table `cafe-prd-dmt-387402.sales.customer_visits`
(
    order_id integer /* オーダーID */
    ,day date /* 来店日 */
    ,customer_id integer /* 顧客ID */
    ,item_name string /* 商品名 */
    ,price integer /* 値段 */
)
