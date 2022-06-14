-----------------------------------
-- Average Zone Shipped Tracking --
-----------------------------------
-- 7 NB HG/Specialty Planner Codes: ('PPRAKASH','BROSEN','MODZER','BNEUBAUER','MWILSON','SSHARAN','JMALAVIYA')
drop table if exists regional_orders;
create local temp table regional_orders
        (action varchar(10)
        ,item_id varchar(6)
        ,region varchar(12)
        ,location_cd varchar(4)
        ,supplier varchar(15)
        ,proposed_qty int
        ,fc_region_need_percent float)
on commit preserve rows;
copy regional_orders
from local 'C:\Users\cmorris10\Downloads\6-13-22_orders.csv'
parser fcsvparser(delimiter = ',');

drop table if exists locations;
create local temp table locations on commit preserve rows as
        select location_key
                ,location_code
        from chewybi.locations l
        where 1=1
                and l.fulfillment_active is true
                and l.location_active_warehouse = 1
                and l.location_warehouse_type = 0
                and l.product_company_description = 'Chewy'
;

-------------------------------------------------------------------------------------
-- Measure average zone impact for orders w/NB HG items vs. orders w/o NB HG items --
-------------------------------------------------------------------------------------
drop table if exists nb_hg_items;
create local temp table nb_hg_items on commit preserve rows as
        select distinct item_id
        from regional_orders
        order by 1
;

drop table if exists item_ship_zones;
create local temp table item_ship_zones on commit preserve rows as
        select olm.order_key
--                ,case when count(distinct location_code) > 1 then 1 else 0 end as is_split
                ,least(1,sum(case when nb.item_id is not null then 1 else 0 end)) as contains_nbhg_item 
                ,min(order_line_released_dttm::date) as order_released_date
                ,avg(actual_zone) as actual_zone
        from chewybi.order_line_measures olm 
        join chewybi.orders o using(order_key)
        join chewybi.shipment_order_line sol on olm.order_line_id=sol.order_line_id
        join locations l on location_key = fulfillment_center_key
        join chewybi.products p on olm.product_key=p.product_key
        left join nb_hg_items nb on p.product_part_number=nb.item_id
        where order_status not in ('X','J')
        group by 1
        having min(order_line_released_dttm::date) between '2022-04-01' and current_date - 1
        order by 1
;

select order_released_date
        ,contains_nbhg_item
        ,avg(actual_zone) as avg_zone_shipped
from item_ship_zones z
group by 1,2
order by 1,2
;

select * from chewybi.order_line_cost_measures where order_line_id='1384020626';
select * from chewybi.customers where customer_key='32565750453';
select * from chewybi.customer_addresses where customer_address_id='59449682';

----------------------------------------------------------------------------------
-- Measure Split% impact for orders w/NB HG items vs. orders w/o NB HG items --
----------------------------------------------------------------------------------
drop table if exists splits;
create local temp table splits on commit preserve rows as
        select olm.order_key
                ,min(order_line_released_dttm::date) as order_released_date
                ,case when count(distinct location_code) > 1 then 1 else 0 end as is_split
        from chewybi.order_line_measures olm 
        join chewybi.orders o using(order_key)
        join locations l on location_key = fulfillment_center_key
        join chewybi.products p on olm.product_key=p.product_key
        where order_status not in ('X','J')
        group by 1
        having min(order_line_released_dttm::date) between current_date - 90 and current_date - 1
;

drop table if exists split_prods;
create local temp table split_prods on commit preserve rows as
        select s.*
                ,product_part_number
        from splits s
        join chewybi.order_line_measures olm using(order_key)
        join chewybi.products p using(product_key)
;

drop table if exists nb_hg_items;
create local temp table nb_hg_items on commit preserve rows as
        select distinct item_id
        from regional_orders
        order by 1
;

with a as (
        select order_released_date
                ,order_key
                ,max(is_split) as is_split
                ,least(1,sum(case when item_id is not null then 1 else 0 end)) as contains_nbhg_item 
        from split_prods
        left join nb_hg_items nb on nb.item_id = product_part_number
        group by 1,2
)
select order_released_date
        ,contains_nbhg_item
        ,sum(is_split) / count(*) as split_rate
        ,sum(is_split) as split_order_count
        ,count(*) as count_orders 
from a 
group by 1,2
order by 1,2;
--)
---- To get top Brands contributing to Splits
--select s.product_part_number
----         p.product_purchase_brand
--        , p.cartonization_flag
--        , count(distinct order_key) as split_order_count
--from split_prods s
--left join ag on ag.item=s.product_part_number
--join chewybi.products p using(product_part_number)
--where case when ag.item is not null then true else false end is true
--        and is_split=1
--group by 1,2
--order by 2 desc
--;

