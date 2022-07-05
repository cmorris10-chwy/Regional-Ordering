--select distinct purchaser_code from chewybi.procurement_document_product_measures where document_order_dttm::date=current_date-1;

drop table if exists exclusion_vendors;
create local temp table exclusion_vendors
        (supplier_cd varchar(15)
        ,min_dollar_amount numeric
        ,min_weight numeric
        ,min_units_per_order numeric)
on commit preserve rows;
copy exclusion_vendors
from local 'C:\Users\cmorris10\OneDrive - Chewy.com, LLC\Projects\Excess Inventory\Compliance Reporting\Vendor_Exception_List.csv'
parser fcsvparser(delimiter = ',');

drop table if exists regional_orders;
create local temp table regional_orders on commit preserve rows as 
        select r.*
                ,case when e.supplier_cd is not null then true else false end as excluded_vendor
        from sandbox_supply_chain.regional_ordering r 
        left join exclusion_vendors e on split_part(r.supplier,'-',1)=e.supplier_cd
        where order_date=current_date-1
;

--select action_
--        ,SUM(proposed_FC_qty) as total_ordered_units
--from regional_orders ro
--join chewybi.products p on ro.item=p.product_part_number
--join chewybi.vendors v on v.vendor_number=split_part(ro.supplier,'-',1)
--where 1=1
----        and coalesce(p.private_label_flag,false) is false
----        and coalesce(v.vendor_direct_import_flag,false) is false
--group by 1
--;

--select * from regional_orders;

drop table if exists yesterdays_orders;
create local temp table yesterdays_orders on commit preserve rows as
        select document_order_dttm::date as document_order_date
                ,purchaser_code
                ,document_number
                ,l.location_code
                ,v.vendor_number
                ,v.vendor_name
                ,pdpm.product_part_number
                ,document_requested_delivery_dttm as RDD
                ,document_expected_receipt_dttm as ERDD
                ,original_quantity
                ,last_version_quantity
                ,outstanding_quantity
                ,total_cost
        from chewybi.procurement_document_product_measures pdpm --Q: Should we first pull RS numbers from Mercury and then PDPM?
        join chewybi.products p using(product_key)
        join chewybi.vendors v using(vendor_key)
        join chewybi.locations l using(location_key)
        where document_order_dttm::date=current_date-1 --Monday or Wednesday
                and deleted_by_users is false
                and document_type = 'Purchase'
                and pdpm.data_source = 'ORACLE'
                and purchaser_code in ('PPRAKASH','BROSEN','MODZER','BNEUBAUER','MWILSON','SSHARAN','JMALAVIYA','MEMILLER')
                and coalesce(document_wms_closed_flag,false) is false
                and coalesce(document_ready_to_reconcile_flag,false) is false
;

drop table if exists orders;
create local temp table orders on commit preserve rows as
        select document_order_date
                ,purchaser_code
                ,location_code
                ,product_part_number
                ,vendor_number
                ,vendor_name
                ,RDD
                ,ERDD
                ,listagg(document_number) as orders_placed
                ,SUM(original_quantity) as original_quantity
                ,SUM(last_version_quantity) as last_version_quantity
                ,SUM(outstanding_quantity) as outstanding_quantity
                ,SUM(total_cost) as total_cost
                ,SUM(original_quantity) / nullifzero(SUM(total_cost)) as cost_per_unit
        from yesterdays_orders
        group by 1,2,3,4,5,6,7,8
        order by 2,3
;

drop table if exists forecast;
create local temp table forecast on commit preserve rows as
        select o.product_part_number
                ,round(SUM(avedemqty),5) as network_avedemqty
        from (select distinct product_part_number
                from orders) o
        join chewy_prod_740.C_ITEMLOCATION cil
                on o.product_part_number=cil.item
        where 1=1
                and split_part(cil.SUPPLIER,'-',1) not in (select distinct location_code from chewybi.locations where fulfillment_active = true and product_company_description = 'Chewy' and location_warehouse_type = 0)
                and split_part(cil.SUPPLIER,'-',1) not in ('','?')
        group by 1
;

drop table if exists compliance_output;
create local temp table compliance_output on commit preserve rows as
        select purchaser_code
                ,p.product_merch_classification1 as MC1
                ,document_order_date
                ,region
                ,location_code
                ,orders_placed
                ,case when ro.action_ is null then 'Manual Order' 
                        when ro.excluded_vendor is true then 'Exclusion Process'
                        else ro.action_ end as "Recommendation"
                ,case   when ro.excluded_vendor is true then 'Ordered via Exclusion process'
                        when ro.action_='APPROVE' and orders_placed is null then 'Not ordered but Approved'
                        when ro.action_='APPROVE' and orders_placed is not null then 'Ordered and Approved'
                        when ro.action_='REJECT' and orders_placed is not null then 'Ordered but Rejected'
                        when ro.action_='REJECT' and orders_placed is null then 'Not ordered and Rejected'
                        when ro.action_='REJECT: Did not Propose' and orders_placed is not null then 'Ordered without Proposal'
                        when ro.action_ is null then 'Ordered without a Proposal'
                        else 'NA'
                        end as action_taken_by_planner
                ,coalesce(ro.item,o.product_part_number) as product_part_number
                ,p.product_abc_code
                ,p.private_label_flag
                ,p.product_discontinued_flag
                ,case when u.cartonization_flag='NO' then true else false end as is_SIOC_item
                ,network_avedemqty
                ,coalesce(o.vendor_number,ro.supplier) as vendor_number
                ,v.vendor_name
                ,v.vendor_direct_import_flag
                ,RDD
                ,o.ERDD as due_date
                ,projected_region_oos
                ,original_quantity - ro.proposed_FC_qty as order_prop_qty_delta
                ,ro.proposed_FC_qty
                ,ro.FC_Region_Need_percent_so99
                ,original_quantity
                ,last_version_quantity
                ,outstanding_quantity
                ,total_cost
                
        --        ,round(oy.fcast_over_lt_rp,5) as fcast_over_lt_rp
        --        ,oy.oo as oo_dayof
        --        ,oy.oh as oh_dayof
                ,oy.oh_oo as IP_dayof
                ,round(oy.outl,0) as OUTL_dayof
                ,round(oy.units_above_outl,0) as excess_units_dayof
                
        --        ,ot.oo as oo_dayafter
        --        ,ot.oh as oh_dayafter
                ,ot.oh_oo as IP_dayafter
                ,round(ot.outl,0) as OUTL_dayafter
                ,round(ot.units_above_outl,0) as excess_units_dayafter
                ,round(ot.units_above_outl - oy.units_above_outl,0) as DoD_excess_change
                ,round((ot.units_above_outl - oy.units_above_outl) / network_avedemqty,2) as network_DOS_for_excess_inventory_change 
        from orders o
        join chewybi.products p using(product_part_number)
        join chewybi.vendors v using(vendor_number)
        left join forecast f on o.product_part_number=f.product_part_number
        left join aad.t_item_uom u
                on wh_id='AVP1'
                and p.product_part_number=u.item_number
        left join sandbox_supply_chain.outl_excess_base oy
                on o.product_part_number=oy.product_part_number
                and oy.snapshot_date=o.document_order_date
        left join sandbox_supply_chain.outl_excess_base ot
                on o.product_part_number=ot.product_part_number
                and ot.snapshot_date=o.document_order_date+1
        full outer join regional_orders ro
                on o.product_part_number=ro.item
                and o.vendor_number=split_part(ro.supplier,'-',1)
                and o.location_code=ro.location
        where 1=1
                and coalesce(p.private_label_flag,false) is false
                and coalesce(v.vendor_direct_import_flag,false) is false
        order by 7
;

select * 
from compliance_output 
where 1=1
        and action_taken_by_planner not in ('Not ordered and Rejected','NA')
order by 1,9,5;--where product_part_number='101303';

---------------------------------------------------------
-- Explore PDP and Sales for items that created excess --
---------------------------------------------------------
drop table if exists merch_historicals;
create local temp table merch_historicals on commit preserve rows as
        select mps.product_part_number
                ,SUM(units_sold) as total_units_sold_t90
                ,AVG(units_sold) as avg_daily_units_sold
                ,SUM(gross_sales) as total_gross_sales_t90
                ,AVG(gross_sales) as avg_gross_sales
                ,SUM(gross_margin) as total_gross_margin_t90
                ,AVG(gross_margin) as avg_gross_margin
                ,SUM(contribution_margin) as total_CM_t90
                ,AVG(contribution_margin) as avg_contribution_margin
                ,SUM(Distinct_PDP_Views) as total_distinct_PDP_views
                ,AVG(Distinct_PDP_Views) as avg_PDP_views
        from chewybi.merch_performance_snapshot mps
        join compliance_output co on co.product_part_number=mps.product_part_number
        where 1=1
                and activity_date between current_date-92 and current_date - 2 --update to be T90
--                and product_part_number in (select distinct product_part_number from compliance_output where Recommendation='REJECT')
        group by 1
        order by 1
;

select co.purchaser_code
        ,MC1
        ,document_order_date
        ,region
        ,location_code
        ,Recommendation
        ,co.product_part_number
        ,product_abc_code
        ,network_avedemqty
        ,orders_placed
        ,order_prop_qty_delta
        ,proposed_qty
        ,fc_region_need_percent
        ,original_quantity
        ,total_cost
        ,excess_units_dayof
        ,excess_units_dayafter
        ,DoD_excess_change
        ,network_DOS_for_excess_inventory_change
        ,total_units_sold_t90
        ,avg_daily_units_sold
        ,total_gross_sales_t90
        ,avg_gross_sales
        ,total_gross_margin_t90
        ,avg_gross_margin
        ,total_CM_t90
        ,avg_contribution_margin
        ,total_distinct_PDP_views
        ,avg_PDP_views
from compliance_output co
left join merch_historicals m using(product_part_number)
order by 1,product_part_number,location_code
