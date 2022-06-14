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

drop table if exists reg;
create local temp table reg on commit preserve rows as
        select 'AVP1' as location_code, 'East' as region union all
        select 'AVP2' as location_code, 'East' as region union all
        select 'CFC1' as location_code, 'Central' as region union all
        select 'CLT1' as location_code, 'South East' as region union all
        select 'DAY1' as location_code, 'Central' as region union all
        select 'DFW1' as location_code, 'Central' as region union all
        select 'EFC3' as location_code, 'East' as region union all
        select 'MCI1' as location_code, 'Central' as region union all
        select 'MCO1' as location_code, 'South East' as region union all
        select 'MDT1' as location_code, 'East' as region union all
        select 'PHX1' as location_code, 'West' as region union all
        select 'RNO1' as location_code, 'West' as region union all
        select 'WFC2' as location_code, 'West' as region
;

drop table if exists item_fc_data;
create local temp table item_fc_data on commit preserve rows as
        select inventory_snapshot_snapshot_dt as snapshot_date
                ,ro.item_id
                ,i.location_code
                ,region
                ,coalesce(i.inventory_snapshot_sellable_quantity,0) as sellable_units
                ,coalesce(p.product_discontinued_flag,false) as product_discontinued_flag
        from (select distinct item_id
                from regional_orders) ro
        join chewybi.inventory_snapshot i
                on ro.item_id=i.product_part_number
        join reg using(location_code)
        join chewybi.product_lifecycle_snapshot p
                on i.inventory_snapshot_snapshot_dt=p.snapshot_date
                and i.product_part_number=p.product_part_number
        where 1=1
                and i.inventory_snapshot_snapshot_dt between '2022-04-01' and current_date-1
        order by 1,2
;

drop table if exists item_reg_data;
create local temp table item_reg_data on commit preserve rows as
        select snapshot_date
                ,item_id
                ,region
                ,case when SUM(sellable_units) > 0 then 0 else 1 end as region_OOS
        from item_fc_data
        where 1=1
                and product_discontinued_flag is false --Only accounting for replenishable items as OOS for a Disco item is expected
        group by 1,2,3
        order by 1,2
;

select * from item_reg_data limit 1000;

select snapshot_date
        ,region
        ,COUNT(*) as number_of_items --HG+Specialty
        ,SUM(region_OOS) as items_OOS_in_region
        ,round(SUM(region_OOS) / COUNT(*),4) as region_OOS_percentage
from item_reg_data
group by 1,2
order by 2,1;


--TODO: HG vs Non-HG OOS rates


