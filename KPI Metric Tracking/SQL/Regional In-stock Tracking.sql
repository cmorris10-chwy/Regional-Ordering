--drop table if exists regional_orders;
--create local temp table regional_orders
--        (action varchar(10)
--        ,item_id varchar(6)
--        ,region varchar(12)
--        ,location_cd varchar(4)
--        ,supplier varchar(15)
--        ,proposed_qty int
--        ,fc_region_need_percent float)
--on commit preserve rows;
--copy regional_orders
--from local 'C:\Users\cmorris10\Downloads\6-13-22_orders.csv'
--parser fcsvparser(delimiter = ',');

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
                ,i.product_part_number
                ,i.location_code
                ,region
                ,case when inventory_snapshot_managed_flag is false and coalesce(i.inventory_snapshot_sellable_quantity,0)<=0 then null else coalesce(i.inventory_snapshot_sellable_quantity,0) end as sellable_units
                ,coalesce(p.product_discontinued_flag,false) as product_discontinued_flag
                ,case when ro.item is not null then true else false end as HG_ordered_item
                ,case when nri.product_part_number is not null then true else false end as non_replenishable_item
        from chewybi.inventory_snapshot i
        join reg using(location_code)
        join chewybi.product_lifecycle_snapshot p
                on i.inventory_snapshot_snapshot_dt=p.snapshot_date
                and i.product_part_number=p.product_part_number
        left join (select distinct item
                from sandbox_supply_chain.regional_ordering
                union
                select distinct item
                from sandbox_supply_chain.cmorris10_history_data_test) ro on i.product_part_number=ro.item --regional_orders) ro on i.product_part_number=ro.item_id
        left join sandbox_supply_chain.non_replenishable_items nri on i.product_part_number=nri.product_part_number
                                                                and nri.snapshot_date=current_date
        where 1=1
                and i.inventory_snapshot_snapshot_dt between '2022-04-01' and current_date-1
                and coalesce(p.product_discontinued_flag,false) is false --Only accounting for replenishable items as OOS for a Disco item is expected
                and coalesce(p.product_dropship_flag,false) is false
        order by 1,2
;
--select * from item_fc_data where snapshot_date='2022-04-01';

drop table if exists item_reg_data;
create local temp table item_reg_data on commit preserve rows as
        select snapshot_date
                ,product_part_number
                ,HG_ordered_item
                ,region
                ,case when zeroifnull(SUM(sellable_units)) <= 0 then 1 else 0 end as region_OOS
        from item_fc_data
        where 1=1
                and non_replenishable_item is false
                and sellable_units is not null
        group by 1,2,3,4
        order by 1,2
;
--select *
--from item_reg_data
--where 1=1
--        and snapshot_date='2022-04-01'
--        and region='Central'
;

--region
select snapshot_date
        ,region
        ,HG_ordered_item
        ,COUNT(*) as total_item_locs --HG+Specialty
        ,SUM(region_OOS) as items_OOS_in_region
        ,round(SUM(region_OOS) / COUNT(*),4) as region_OOS_percentage
from item_reg_data
group by 1,2,3
order by 2,1;

--network
drop table if exists item_network_data;
create local temp table item_network_data on commit preserve rows as
        select snapshot_date
                ,product_part_number
                ,HG_ordered_item
                ,case when zeroifnull(SUM(sellable_units)) <= 0 then 1 else 0 end as network_OOS
        from item_fc_data
        where 1=1
                and non_replenishable_item is false
                and sellable_units is not null
        group by 1,2,3
        order by 1,2
;

select snapshot_date
        ,HG_ordered_item
        ,COUNT(*) as total_items --HG+Specialty
        ,SUM(network_OOS) as items_OOS_in_network
        ,round(SUM(network_OOS) / COUNT(*),4) as network_OOS_percentage
from item_network_data
group by 1,2
order by 2,1;