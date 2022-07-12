--- REGIONAL OOS 
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

drop table if exists reg_oos;
create local temp table reg_oos on commit preserve rows as
        with regional_state as (
                select inventory_snapshot_snapshot_dt as inventory_date
                        ,i.product_part_number
                        ,parent_company
                        ,region
                        ,product_published_flag
                        ,case when zeroifnull(sum(inventory_snapshot_sellable_quantity)) > 0 then true else false end as is_instock
                from chewybi.inventory_snapshot i
                join reg using(location_code)
                join chewybi.products p using(product_part_number)
                where 1=1
                        and inventory_snapshot_snapshot_dt > current_date-90
                        and product_merch_classification1 in ('Hard Goods','Specialty')
                        and coalesce(product_discontinued_flag,false) is false
                        and coalesce(private_label_flag,false) is false
                        and coalesce(product_dropship_flag,false) is false
                group by 1,2,3,4,5
        )
        , conditions as (
                select inventory_date
                        ,product_part_number
                        ,parent_company
                        ,region
                        ,product_published_flag
                        ,is_instock
                        ,conditional_change_event(is_instock) over (partition by product_part_number order by inventory_date desc) as change_event
                from regional_state
                where product_part_number in (select product_part_number from regional_state where is_instock is false and inventory_date=current_date)
        )
        select c.product_part_number
                ,case when nri.product_part_number is null then 'true' else nri.Non_Replen_Cat end as is_replenishable_item
                ,parent_company
                ,region
                ,product_published_flag
                ,COUNT(*) as days_OOS
        from conditions c
        left join sandbox_supply_chain.non_replenishable_items nri
                on nri.snapshot_date=current_date
                and c.product_part_number=nri.product_part_number
        where change_event=0
        group by 1,2,3,4,5
        order by 6 desc,1,4;

drop table if exists reg_oos_vendors;
create local temp table reg_oos_vendors on commit preserve rows as
        select r.product_part_number
                ,r.region
                ,cil.location
                ,supplier
        from reg_oos r
        join reg using(region)
        join chewy_prod_740.C_ITEMLOCATION cil
                on r.product_part_number=cil.item
                and reg.location_code=cil.location
        where 1=1
                and is_replenishable_item = 'true'
;

drop table if exists last_opportunity;
create local temp table last_opportunity on commit preserve rows as
        select r.product_part_number
                ,r.location
                ,r.region
                ,max(order_date) as last_order_date
                ,COUNT(*) as opportunities_to_order_count
        from reg_oos_vendors r
        left join sandbox_supply_chain.regional_ordering o
                on r.product_part_number=o.item
                and r.location=o.location
                and r.supplier=o.supplier
        group by 1,2,3
        order by 1,3,2
;

--Ordering recommendations based on the most recent proposal for the item-FC
drop table if exists reg_ord_recs;
create local temp table reg_ord_recs on commit preserve rows as 
        select lo.product_part_number
                ,lo.location
                ,lo.region
                ,o.order_date
                ,o.supply_planner
                ,p.parent_company
                ,o.supplier
                ,o.MC1
                ,o.action_
                ,o.proposed_FC_qty
                ,o.FC_Region_Need_percent_so99
                ,o.region_proposed_cummulative_quantity
                ,o.total_region_SS
                ,o.total_region_IP
                ,o.total_region_NEED
                ,o.fc_need_rank_in_region
        from last_opportunity lo
        join chewybi.products p using(product_part_number)
        left join sandbox_supply_chain.regional_ordering o
                on lo.product_part_number=o.item
                and lo.location=o.location
                and lo.last_order_date=o.order_date
        order by 1,3,2
;

drop table if exists open_orders;
create local temp table open_orders on commit preserve rows as
        select   c.item as product_part_number
                ,c.location
                ,sum(onordqty) as oo_units
                ,min(date(year||'-'||month||'-'||day)) as earliest_PO_ERDD
                ,listagg(lotID) as open_PO_list
        from reg_ord_recs r
        join chewy_prod_740.C_ONORDER_DET c
                on r.product_part_number=c.item
                and r.location=c.location
        where 1=1
                and (lotID like '%%RS%%' or lotID like '%%TR%%')
                and date(year||'-'||month||'-'||day) >= current_date
        group by 1,2
;

drop table if exists combined_data;
create local temp table combined_data on commit preserve rows as
        select ro.*
                ,oo.oo_units
                ,oo.earliest_PO_ERDD
                ,oo.open_PO_list
        from reg_ord_recs ro
        left join open_orders oo
                using(product_part_number,location)
        order by product_part_number,region,location;

select product_part_number
        ,region
        ,zeroifnull(SUM(oo_units)) as region_OO
        ,MIN(earliest_PO_ERDD) as projected_region_instock_date
from combined_data
group by 1,2
having zeroifnull(SUM(oo_units)) = 0
order by 1,2;

select *
from sandbox_supply_chain.regional_ordering
where item='100864';