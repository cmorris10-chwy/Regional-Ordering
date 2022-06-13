-----------------------------------
-- Average Zone Shipped Tracking --
-----------------------------------
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
from local 'C:\Users\cmorris10\Downloads\6-8-22_orders.csv'
parser fcsvparser(delimiter = ',');

drop table if exists order_history;
create local temp table order_history on commit preserve rows as
        select product_part_number
                ,sol.order_line_id
                ,l.location_code
                ,common_date_dttm::date as order_date
                ,shipment_quantity
                ,actual_zone
        --        ,median(actual_zone) over (partition by location_code,common_date_dttm::date) as med_FC_zone_shipped
        from chewybi.shipment_order_line sol
        join chewybi.order_line_cost_measures olcm
                on sol.order_line_id=olcm.order_line_id
        join chewybi.products p using(product_key)
        join chewybi.locations l on l.location_key=olcm.fulfillment_center_key
        join chewybi.common_date cd on cd.common_date_key=olcm.order_placed_date_key
        join (select distinct item_id from regional_orders) ro on p.product_part_number=ro.item_id
        where 1=1
        --        and p.product_part_number='141529'
                and l.fulfillment_active is true
                and l.location_active_warehouse = 1
                and l.location_warehouse_type = 0
                and l.product_company_description = 'Chewy'
                and common_date_dttm::date between current_date -90 and current_date-1
;

select product_part_number
--        location_code
        ,date_trunc('week',order_date) as order_week
        ,round(avg(actual_zone),0) as avg_zone_shipped
from order_history
group by 1,2--,3
order by 1,2--,3
;

select * from chewybi.order_line_cost_measures where order_line_id='1384020626';
select * from chewybi.customers where customer_key='32565750453';
select * from chewybi.customer_addresses where customer_address_id='59449682';
