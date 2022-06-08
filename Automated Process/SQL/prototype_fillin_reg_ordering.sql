-- run this query in Merucry DB and then save the output to CSV
select item_id
from mercury.item_attr
where attr_cd='NEW_ITEM'
        and item_id not like 'RX%%';

-- Switch back to Chewy DB and update the file directory
drop table if exists new_items;
create local temp table new_items
        (item_id varchar(7))
on commit preserve rows;
copy new_items
from local 'C:\users\cmorris10\Downloads\new_items.csv'
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

drop table if exists inv_base;
create local temp table inv_base on commit preserve rows as
        select i.product_part_number
                ,i.location_code
                ,inventory_snapshot_managed_flag as is_assorted
                ,r.region
                ,inventory_snapshot_sellable_quantity as current_on_hand_FC
        from chewybi.inventory_snapshot i
        join chewybi.products p using(product_key)
        join chewybi.locations l using(location_key)
        join reg r on l.location_code=r.location_code
        where 1=1
                and inventory_snapshot_snapshot_dt = current_date
                and l.fulfillment_active is true
                and l.location_active_warehouse = 1
                and l.location_warehouse_type = 0
                and l.product_company_description = 'Chewy'
;

drop table if exists products;
create local temp table products on commit preserve rows as
        select ib.product_part_number
                ,p.product_merch_classification1 as MC1
                ,p.product_merch_classification2 as MC2
                ,p.product_merch_classification3 as MC3
                ,product_abc_code
                ,product_discontinued_flag
                ,product_published_flag
                ,case when ni.item_id is not null then true else false end as is_NEW_ITEM
        from (select distinct product_part_number
                from inv_base) ib
        join chewybi.products p using(product_part_number)
        left join new_items ni on ib.product_part_number=ni.item_id
;

drop table if exists reg_inv;
create local temp table reg_inv on commit preserve rows as
        select product_part_number
                ,region
                ,SUM(current_on_hand_FC) as current_on_hand_region
        from inv_base
        group by 1,2;

drop table if exists network_inv;
create local temp table network_inv on commit preserve rows as
        select product_part_number
                , SUM(current_on_hand_region) as current_on_hand_network
        from reg_inv
        group by 1
        order by 1
;

drop table if exists reg_fcast;
create local temp table reg_fcast on commit preserve rows as
        with base as (
                select product_part_number
                        ,r.region
                        ,date_trunc('week', forecast_current_forecast_dt) as week
                        ,sum(coalesce(fc.forecast_current_manual_forecast_quantity,fc.forecast_current_statistical_forecast_quantity)) as forecast_region
                from chewybi.forecast_current fc
                join chewybi.products p using(product_key)
                join chewybi.locations l using(location_key)
                join reg r on l.location_code=r.location_code
                where 1=1
                        and fc.forecast_current_forecast_dt between current_date and '2022-12-31'
                        and l.fulfillment_active is true
                        and l.location_active_warehouse = 1
                        and l.location_warehouse_type = 0
                        and l.product_company_description = 'Chewy'
                        group by 1,2,3
                )
        select *
                ,avg(forecast_region) over(partition by product_part_number,region,week) as avg_daily_forecast_week
                ,sum(forecast_region) over(partition by product_part_number,region order by week asc) as cumulative_forecast_region
                ,dense_rank() over(partition by product_part_number,region order by week asc) as week_rank_region
        from base
;

drop table if exists network_fcast;
create local temp table network_fcast on commit preserve rows as
        select product_part_number
                , week
                , SUM(forecast_region) as forecast_network
        from reg_fcast
        group by 1,2
;

drop table if exists reg_oo_weekly;
create local temp table reg_oo_weekly on commit preserve rows as
        select
                c.item as product_part_number
                ,r.region
                ,date_trunc('week',date(year||'-'||month||'-'||day)) as week
                ,sum(onordqty) as oo_region
        from chewy_prod_740.C_ONORDER_DET c
        join chewybi.products p on product_part_number = item
        join chewybi.procurement_document_measures pdm on document_number = lotid
        join chewybi.locations l using(location_key)
        join reg r on l.location_code=r.location_code
        where 1=1
                and (lotID like '%%RS%%' or lotID like '%%TR%%')
                and date(year||'-'||month||'-'||day) >= current_date
        group by 1,2,3
;

drop table if exists reg_oo;
create local temp table reg_oo on commit preserve rows as
        select product_part_number
                , region
                , sum(oo_region) as oo_region
        from reg_oo_weekly
        group by 1,2
;

drop table if exists network_oo_weekly;
create local temp table network_oo_weekly on commit preserve rows as
        select product_part_number
                , week
                , SUM(oo_region) as oo_network
        from reg_oo_weekly
        group by 1,2
;

drop table if exists network_oo;
create local temp table network_oo on commit preserve rows as
        select product_part_number
                , SUM(oo_region) as oo_network
        from reg_oo
        group by 1
;

drop table if exists reg_base;
create local temp table reg_base on commit preserve rows as
        select r.product_part_number
                ,r.region
                ,r.current_on_hand_region
                ,n.current_on_hand_network
                ,round(n.current_on_hand_network / zeroifnull(o.avg_daily_forecast),2) as current_on_hand_DOS
                ,ro.oo_network as current_on_order_network
                ,round(ro.oo_network / zeroifnull(o.avg_daily_forecast),2) as current_on_order_DOS                        
                ,o.avg_daily_forecast
                ,f.week
                ,f.forecast_region
                ,f.cumulative_forecast_region
                ,f.avg_daily_forecast_week
                ,f.week_rank_region
                ,nf.forecast_network
                ,oo.oo_region as oo_region_for_week
                ,noo.oo_network as oo_network_for_week
        from reg_inv r
        join network_inv n using(product_part_number)
        join sandbox_supply_chain.outl_excess_base o 
                on o.snapshot_date=current_date --update to use yesterday's OUTL information if running script before 11AM as the data is not yet updated by batch run
                and r.product_part_number=o.product_part_number
        left join network_oo ro on r.product_part_number=ro.product_part_number
        left join reg_fcast f on f.product_part_number=r.product_part_number
                                and r.region=f.region
        left join reg_oo_weekly oo on r.product_part_number=oo.product_part_number
                        and r.region=oo.region
                        and f.week=oo.week
        left join network_fcast nf on r.product_part_number=nf.product_part_number
                                        and f.week=nf.week
        left join network_oo_weekly noo on r.product_part_number=noo.product_part_number
                                and f.week=noo.week
;

--Get future states at regional level
drop table if exists reg_fin;
create local temp table reg_fin on commit preserve rows as
        select region,
                sandbox_supply_chain.inventory_projection(product_part_number::varchar,
                week::date,
                current_on_hand_region::int,
                forecast_region::float,
                oo_region_for_week::int) over(partition by product_part_number,region)
        from reg_base
; 

--Get future inventory states at network level
drop table if exists network_fin;
create local temp table network_fin on commit preserve rows as
       select sandbox_supply_chain.inventory_projection(product_part_number::varchar,
                week::date,
                current_on_hand_network::int,
                forecast_network::float,
                oo_network_for_week::int) over(partition by product_part_number)
        from (select distinct product_part_number
                        , week
                        , current_on_hand_network
                        , forecast_network
                        , oo_network_for_week
                from reg_base) r
;

drop table if exists final;
create local temp table final on commit preserve rows as
        select f.region
                ,f.product_part_number
                ,p.product_abc_code
                ,p.is_NEW_ITEM
                ,r.current_on_hand_network
                ,r.current_on_order_network
                ,f.week
                ,f.oo as projected_on_order_region
                ,f.inv as projected_on_hand_region
                ,f.forecast as region_forecast
                ,nf.oo as projected_on_order_network
                ,nf.inv as projected_on_hand_network
                ,nf.forecast as network_forecast
                ,zeroifnull(r.avg_daily_forecast) as avg_daily_forecast
                ,r.forecast_region
                ,zeroifnull(r.avg_daily_forecast_week) as avg_daily_forecast_week
        from reg_fin f
        join products p using(product_part_number)
        join network_fin nf on f.product_part_number=nf.product_part_number
                        and f.week=nf.week
        join reg_base r on f.region=r.region
                        and f.product_part_number=r.product_part_number
                        and f.week=r.week
;

--Get full item-FC set to get all OUTLs as proposals don't account for all item-FCs
--Only look at assorted items as non-assorted items do not get replenished; therefore no OUTL
drop table if exists direct_props;
create local temp table direct_props on commit preserve rows as
        select cil.item
                ,cil.location as location
                ,qty as proposed_qty
                ,tpeh.supplier
                ,v.vendor_name
                ,cil.MINRESINT as review_period
                ,cil.MINRESLOT as MOQ
                ,prundate::date
                ,duedate::date
                ,(min(duedate) over (partition by cil.item, region))::date as min_ERDD_region
                ,reg.region
                ,status
        from chewy_prod_740.C_ITEMLOCATION cil
        join reg on reg.location_code=cil.location
        left join chewy_prod_740.t_proposals_edit tpeh
                on tpeh.item=cil.item
                and left(tpeh.destwhs,4)=cil.location
                and tpeh.supplier=cil.supplier
                and tpeh.prundate = current_date
                and status != 'X' --Fill in proposals
        left join chewybi.vendors v on split_part(tpeh.supplier,'-',1)=v.vendor_number
        where 1=1
                and (tpeh.supplier is null or tpeh.supplier not in (select distinct location_code from reg)) --We do not want to order self-transfers
;

drop table if exists fillin_props;
create local temp table fillin_props on commit preserve rows as
        select item
                ,destwhs as location
                ,tpeh.supplier
                ,v.vendor_name
                ,prundate::date
                ,duedate::date
                ,(min(duedate) over (partition by item, region))::date as min_ERDD_region
                ,reg.region
                ,status
                ,qty
        from chewy_prod_740.t_proposals_edit tpeh
        join reg on reg.location_code=split_part(destwhs,'_',1)
        join chewybi.vendors v on split_part(tpeh.supplier,'-',1)=v.vendor_number
        where 1=1
                and tpeh.supplier not in (select distinct location_code from reg)
                and tpeh.prundate = current_date
                and status = 'X'
        order by 1,2
;

drop table if exists tunnel;
create local temp table tunnel on commit preserve rows as
        select props.item
                ,props.location
                ,props.proposed_qty
                ,props.supplier
                ,props.vendor_name
                ,props.review_period
                ,props.MOQ
                ,prundate
                ,coalesce(duedate,min_ERDD_region) as duedate
                ,min_ERDD_region
                ,region
                ,status
                ,greatest(0,stkmin) as FC_OUTL_so99
                ,greatest(0,stkono) as FC_IP_so99
                ,greatest(0,stkmin)-greatest(0,stkono) as FC_NEED_so99 
                ,left(whouse,4) as tunn_fc -- stkono is the IP and stkmin is the SS(outl) in SO99
                ,row_number() over (partition by props.item,region order by greatest(0,stkmin)-greatest(0,stkono) desc) as fc_need_rank_in_region --Rank of the proposal's need against other FC proposals in the same Region for the item
                ,row_number() over (partition by props.item order by greatest(0,stkmin)-greatest(0,stkono) desc) as fc_need_rank_in_network --Rank of the proposal's need against all FC proposals in the network for the item
        from props
        join chewy_prod_740.T_TUNNEL_D_STK_UNION stk on stk.item = props.item and coalesce(props.duedate,props.min_ERDD_region) = date and left(whouse,4) = props.location
        where 1=1
;
       
--Get region level metrics for each item
drop table if exists reg_tunnel;
create local temp table reg_tunnel on commit preserve rows as
        select item
                ,t.region
                ,sum(greatest(0,FC_OUTL_so99)) as total_reg_OUTL_so99
                ,sum(greatest(0,FC_IP_so99)) as total_reg_IP_so99
                ,sum(greatest(0,FC_OUTL_so99))-sum(greatest(0,FC_IP_so99)) as total_reg_NEED_so99
        from tunnel t
        group by 1,2
;

--Get network level metrics for each item
drop table if exists network_tunnel;
create local temp table network_tunnel on commit preserve rows as
        select item
                ,sum(greatest(0,total_reg_OUTL_so99)) as total_network_OUTL_so99
                ,sum(greatest(0,total_reg_IP_so99)) as total_network_IP_so99
                ,sum(greatest(0,total_reg_OUTL_so99))-sum(greatest(0,total_reg_IP_so99)) as total_network_NEED_so99
        from reg_tunnel
        group by 1
;

drop table if exists full_tunnel;
create local temp table full_tunnel on commit preserve rows as
        select t.item
                ,fin.product_abc_code
                ,fin.is_NEW_ITEM
                ,t.region
                ,t.location
                ,round(fin.avg_daily_forecast,3) as avg_daily_forecast
                ,t.supplier
                ,t.MOQ
                ,t.review_period
                ,t.prundate as release_date
                ,t.duedate as ERDD --fill in for item-FCs without a proposal
                ,fin.projected_on_hand_region
                ,fin.projected_on_order_region
                ,fin.projected_on_hand_network
                ,fin.projected_on_order_network
                ,t.FC_OUTL_so99
                ,t.FC_IP_so99
                ,t.FC_NEED_so99
                ,rt.total_reg_OUTL_so99
                ,rt.total_reg_IP_so99
                ,rt.total_reg_NEED_so99
                ,nt.total_network_OUTL_so99
                ,nt.total_network_IP_so99
                ,nt.total_network_NEED_so99
                
                ,fin.current_on_hand_network
                ,fin.current_on_order_network
                ,case when zeroifnull(fin.projected_on_hand_region) <= 0 then true else false end as projected_region_oos
                ,case when zeroifnull(fin.projected_on_hand_network) <= 0 then true else false end as projected_network_oos
                
                ,outl.outl as current_OUTL_network_calcd --We use this OUTL as the SO99 OUTL is not the OUTL used to calculate Excess Inventory
                ,outl.oh_oo as current_IP_network_calcd
                ,outl.outl-outl.oh_oo as current_NEED_network
                
                ,t.proposed_qty as proposed_FC_qty
                ,outl.oh_oo+sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc) as current_network_IP_with_proposed_qty_cummulative_calcd --What will IP be if we place order?
                ,(outl.oh_oo+sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc)) - outl.outl as current_network_excess_created_cummulative_calcd --Positive value when Excess inventory created. Calculate the running SUM of Delta(IP-OUTL) to see when it begins to become excess at the network level
                
                ,total_network_IP_so99+sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc) as current_network_IP_with_proposed_qty_cummulative_so99
                ,(total_network_IP_so99+sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc)) - total_network_OUTL_so99 as current_network_excess_created_cummulative_so99
                
                ,t.fc_need_rank_in_region --based on SO99 
                ,t.fc_need_rank_in_network --based on SO99
                
                ,sum(t.proposed_qty) over (partition by t.item,t.region order by fc_need_rank_in_region asc) as region_proposed_QTY_cummulative
                ,total_reg_NEED_so99 - sum(t.proposed_qty) over (partition by t.item,t.region order by fc_need_rank_in_region asc) as region_need_left_after_order_cummulative_so99 --excess inventory when value is negative
                ,sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc) as network_proposed_QTY_cummulative
                ,total_network_NEED_so99 - sum(t.proposed_qty) over (partition by t.item order by fc_need_rank_in_network asc) as network_need_left_after_order_cummulative_so99 --excess inventory when value is negative
--                ,sum(t.FC_NEED) over (partition by t.item order by fc_need_rank_in_network asc) as network_NEED_cummulative
        from tunnel t
        join reg_tunnel rt on t.item=rt.item
                        and t.region=rt.region
        join network_tunnel nt on t.item=nt.item
        left join sandbox_supply_chain.outl_excess_base outl on t.item=outl.product_part_number
                                                                and outl.snapshot_date=current_date
        left join final fin 
                on fin.product_part_number = t.item 
                and fin.week = date_trunc('week',t.duedate) 
                and fin.region = t.region
        order by item,region,fc_need_rank_in_region
;

drop table if exists need_calcs;
create local temp table need_calcs on commit preserve rows as
        with x as (
        select *
                ,case when region_need_left_after_order_cummulative_so99 >= 0 then 1 --If there is still regional NEED after this proposal line is ordered then it is 100% needed
                        when proposed_FC_qty >= -1*region_need_left_after_order_cummulative_so99 then (proposed_FC_qty+region_need_left_after_order_cummulative_so99) / proposed_FC_qty 
                        else 0
                        end as FC_Region_need_Percent_so99
                ,case when network_need_left_after_order_cummulative_so99 >= 0 then 1 --If there is still network NEED then the proposal order is 100% needed
                        when proposed_FC_qty >= -1*network_need_left_after_order_cummulative_so99 then (proposed_FC_qty+network_need_left_after_order_cummulative_so99) / proposed_FC_qty
                        else 0
                        end as FC_Network_need_percent_cummulative_so99
                ,case when network_need_left_after_order_cummulative_so99 >= 0 then 0 --when there is still network NEED then there is 0 units excess DOS
                        else (-1*(total_network_NEED_so99 - network_need_left_after_order_cummulative_so99)) / nullifzero(avg_daily_forecast) --Take delta of current Network Need - network_need after order is placed to get how much un-needed units were purchased
                        end as network_excess_created_from_order_cummulative_DOS_so99
                ,network_proposed_QTY_cummulative / nullifzero(avg_daily_forecast) as proposed_ordered_units_cummulative_DOS_so99
        from full_tunnel ft
--        where item='266944'
        order by item,region,fc_need_rank_in_region
        )
        select *
                , case when abs(network_excess_created_from_order_cummulative_DOS_so99) <=30 then '0-30' else '31+' end as network_excess_created_cummulative_DOS_bucket--
                , case when abs(proposed_ordered_units_cummulative_DOS_so99) <= 60 then '0-60' else '61+' end as proposed_ordered_units_cummulative_DOS_bucket
        from x
        order by item,region,fc_need_rank_in_region
;

--Add actions to each proposal line (KEEP/CANCEL)
select v.vendor_purchaser_code as "Supply Planner"
        ,MC1
        ,case   when supplier is null then 'REJECT: Did not Propose'
                when projected_region_oos is true and region_need_left_after_order_cummulative_so99 < 0 and fc_need_rank_in_region = 1 then 'APPROVE' --Region need is satisfied by rank=1 proposal released
                when FC_Region_need_Percent_so99 < 0.5 then 'REJECT'
                when FC_Region_need_Percent_so99 >= 0.5 then 'APPROVE' --explore these items impact on Network
                else null
                end as action_
        ,item
        ,region
        ,location
        ,p.product_abc_code
        ,p.is_NEW_ITEM
        ,avg_daily_forecast
        ,supplier
        ,v.vendor_name
        ,v.vendor_distribution_method
        ,release_date
        ,ERDD
        ,MOQ
        
        ,projected_region_oos
        ,projected_network_oos
        ,round(FC_NEED_so99,2) as FC_NEED_so99
        ,proposed_FC_qty
        ,round(FC_Region_need_Percent_so99,2) as FC_Region_need_Percent_so99
        ,region_proposed_QTY_cummulative
        ,round(total_reg_OUTL_so99,3) as total_reg_OUTL_so99
        ,round(total_reg_IP_so99,3) as total_reg_IP_so99
        ,round(total_reg_NEED_so99,3) as total_reg_NEED_so99
        ,fc_need_rank_in_region
        ,case when region_need_left_after_order_cummulative_so99 < 0 then 0 else round(region_need_left_after_order_cummulative_so99,2) end as region_need_left_after_order_cummulative_so99
        ,case when region_need_left_after_order_cummulative_so99 > 0 then 0 else abs(round(region_need_left_after_order_cummulative_so99,2)) end as "Region Excess Units created from Order - cummulative"
        
        ,network_proposed_QTY_cummulative
        ,round(FC_Network_need_percent_cummulative_so99,2) as FC_Network_need_percent_cummulative_so99
        ,round(total_network_OUTL_so99,3) as total_network_OUTL_so99
        ,round(total_network_IP_so99,3) as total_network_IP_so99
        ,round(total_network_NEED_so99,3) as total_network_NEED_so99
        ,fc_need_rank_in_network
        ,current_network_IP_with_proposed_qty_cummulative_so99
        ,case when network_need_left_after_order_cummulative_so99 < 0 then 0 else round(network_need_left_after_order_cummulative_so99,2) end as network_need_left_after_order_cummulative_so99
        ,case when current_network_excess_created_cummulative_so99 < 0 then 0 else round(current_network_excess_created_cummulative_so99,2) end as "Network Excess Units created from Order - cummulative"
        ,abs(round(network_excess_created_from_order_cummulative_DOS_so99,0)) as network_excess_created_from_order_cummulative_DOS
        ,network_excess_created_cummulative_DOS_bucket
        ,round(proposed_ordered_units_cummulative_DOS_so99,0) as proposed_ordered_units_cummulative_DOS
        ,proposed_ordered_units_cummulative_DOS_bucket
        ,case when MOQ / nullifzero(avg_daily_forecast) > 60 then true else false end as has_high_MOQ
from need_calcs n
left join chewybi.vendors v on v.vendor_number=split_part(supplier,'-',1)
join products p on n.item=p.product_part_number
where 1=1
        and supplier is not null --Remove item-FCs that did not have a proposal today.
order by 1,4,region,fc_need_rank_in_region;