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
--                        and i.product_part_number='100851'
                group by 1,2,3,4,5
        )
        , conditions as (
                select inventory_date
                        ,product_part_number
                        ,parent_company
                        ,region
                        ,product_published_flag
                        ,is_instock
                        ,conditional_change_event(is_instock) over (partition by product_part_number,region order by inventory_date desc) as change_event
                from regional_state
                where 1=1
        )
        , oos_regions as (
                select product_part_number
                        ,region
                from regional_state
                where is_instock is false
                        and inventory_date = current_date
        )
        select c.product_part_number
                ,case when nri.product_part_number is null then 'true' else nri.Non_Replen_Cat end as is_replenishable_item
                ,parent_company
                ,c.region
                ,product_published_flag
                ,COUNT(*) as days_OOS
        from conditions c
        join oos_regions oos
                on c.product_part_number=oos.product_part_number
                and c.region=oos.region
        left join sandbox_supply_chain.non_replenishable_items nri
                on nri.snapshot_date=current_date
                and c.product_part_number=nri.product_part_number
        where change_event=0
        group by 1,2,3,4,5
        order by 6 desc,1,4
;

--select supplier,product_part_number,location,ro.region,*
--from reg_oos ro
--join reg using(region)
--join chewy_prod_740.C_ITEMLOCATION cil on ro.product_part_number=cil.item
--        and cil.location=reg.location_code;

drop table if exists last_opportunity;
create local temp table last_opportunity on commit preserve rows as
        select reg.product_part_number
                ,o.region
                ,MAX(order_date) as last_regional_proposal_date
        from sandbox_supply_chain.regional_ordering o
        join reg_oos reg 
                on o.item=reg.product_part_number
                and o.region=reg.region
        group by 1,2
;

drop table if exists reg_oos_recs;
create local temp table reg_oos_recs on commit preserve rows as
        select roos.product_part_number
                ,cil.location
                ,reg.region
                ,roos.days_OOS as days_Region_OOS
                ,ro.order_date
                ,v.vendor_purchaser_code as "Supply Planner"
                ,cil.supplier as current_primary_supplier
                ,v.vendor_direct_import_flag
                ,ro.supplier as last_proposed_for_supplier
                ,ro.action_
                ,ro.projected_region_oos
                ,ro.proposed_FC_qty
                ,ro.ERDD
                ,ro.FC_Region_Need_percent_so99
                ,ro.region_proposed_cummulative_quantity
                ,ro.total_region_SS
                ,ro.total_region_IP
                ,ro.total_region_NEED
                ,ro.fc_need_rank_in_region
        from reg_oos roos
        join reg using(region)
        join chewy_prod_740.C_ITEMLOCATION cil
                on roos.product_part_number=cil.item
                and reg.location_code=cil.location
        left join last_opportunity lo
                on roos.product_part_number=lo.product_part_number
                and roos.region=lo.region
        left join sandbox_supply_chain.regional_ordering ro
                on roos.product_part_number=ro.item
                and cil.location=ro.location
                and lo.last_regional_proposal_date=ro.order_date
        left join chewybi.vendors v
                on split_part(cil.supplier,'-',1)=v.vendor_number
        order by 1,3,2
; 
--select * from reg_oos_recs where product_part_number='101509';

drop table if exists open_orders;
create local temp table open_orders on commit preserve rows as
        select   r.product_part_number
                ,r.location
                ,sum(onordqty) as oo_units
                ,min(date(year||'-'||month||'-'||day)) as earliest_PO_ERDD
                ,listagg(lotID) as open_PO_list
        from reg_oos_recs r
        join chewy_prod_740.C_ONORDER_DET c
                on r.product_part_number=c.item
                and r.location=c.location
        where 1=1
                and (lotID like '%%RS%%' or lotID like '%%TR%%')
                and date(year||'-'||month||'-'||day) >= current_date
        group by 1,2
;
--select * from open_orders where product_part_number='102275';

drop table if exists combined_data;
create local temp table combined_data on commit preserve rows as
        select ro.*
                ,oo.oo_units
                ,oo.earliest_PO_ERDD
                ,oo.open_PO_list
        from reg_oos_recs ro
        left join open_orders oo
                using(product_part_number,location)
        order by product_part_number,region,location
;

with none_oo as (
        select product_part_number
                ,region
                ,zeroifnull(SUM(oo_units)) as region_OO
--                ,MIN(earliest_PO_ERDD) as projected_region_instock_date
--                ,MAX(order_date) as most_recent_proposal_date
        from combined_data
        group by 1,2
        having zeroifnull(SUM(oo_units)) = 0
        order by 1,2
)
, combo as (
        select p.product_merch_classification1 as MC1
                ,p.parent_company
                ,oo.product_part_number
--                ,p.private_label_flag
--                ,cd.vendor_direct_import_flag
                ,oo.region
                ,cd.location
                ,cd.days_Region_OOS
                ,ag."Issue Class 1"
                ,ag."Issue Class 2"
                ,cd.order_date as last_chance_to_order_in_region
                ,cd."Supply Planner" --this is the Planner for the current Primary Vendor
                ,cd.current_primary_supplier
                ,cd.last_proposed_for_supplier
                ,cd.action_
                ,cd.projected_region_oos
                ,cd.proposed_FC_qty
                ,cd.FC_Region_Need_percent_so99
                ,cd.total_region_NEED
                ,cd.total_region_SS
                ,cd.total_region_IP
                ,cd.fc_need_rank_in_region
        from none_oo oo
        join combined_data cd
                using(product_part_number,region)
        join chewybi.products p 
                on oo.product_part_number=p.product_part_number
        left join sandbox_supply_chain.cmorris10_assortment_gap_buckets ag
                on ag.snapshot_date=current_date
                and oo.product_part_number=ag.replenishable_item
                and cd.location=ag.location
        where coalesce(private_label_flag,false) is false
                and coalesce(vendor_direct_import_flag,false) is false
)
select case when "Supply Planner" = 'DAPATEL' then 'Primary thru Distro'
            when "Issue Class 1" in ('One-Time-Buy','Dropship') then 'OTB/Dropship'
            when "Issue Class 2" = 'Future Availability or Launch Date' then 'Future Availability or Launch Date' 
            when "Issue Class 2" = 'Vendor being Setup' then 'Vendor being Setup'
            when "Supply Planner" is null then 'No Available Vendor' --this can be for no SPA or supplier-location not enabled
            when "Supply Planner" not in ('PPRAKASH','BROSEN','MODZER','BNEUBAUER','MWILSON','SSHARAN','JMALAVIYA','MEMILLER') then 'Non-HG/Specialty Planner'
            else null
        end as "Grouping"
        , *
from combo
order by product_part_number, region, location
;

------------------------------------------------------
select *
from sandbox_supply_chain.regional_ordering
where item='101089';