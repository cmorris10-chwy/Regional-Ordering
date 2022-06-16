select distinct purchaser_code from chewybi.procurement_document_product_measures where document_order_dttm::date=current_date-1;

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

select purchaser_code
        ,p.product_merch_classification1 as MC1
        ,document_order_date
        ,region
        ,location_code
        ,orders_placed
        ,case when ro.action is null then 'Manual Order' else ro.action end as "Recommendation"
        ,o.product_part_number
        ,p.product_abc_code
        ,p.private_label_flag
        ,p.product_discontinued_flag
        ,case when u.cartonization_flag='NO' then true else false end as is_SIOC_item
        ,o.vendor_number
        ,v.vendor_name
        ,v.vendor_direct_import_flag
        ,RDD
        ,ERDD
        ,original_quantity - ro.proposed_qty as order_prop_qty_delta
        ,ro.proposed_qty
        ,ro.fc_region_need_percent
        ,original_quantity
        ,last_version_quantity
        ,outstanding_quantity
        ,total_cost
        
        ,round(oy.fcast_over_lt_rp,5) as fcast_over_lt_rp
        ,oy.oo as oo_dayof
        ,oy.oh as oh_dayof
        ,oy.oh_oo as IP_dayof
        ,round(oy.outl,0) as OUTL_dayof
        ,round(oy.units_above_outl,0) as excess_units_dayof
        
        ,ot.oo as oo_dayafter
        ,ot.oh as oh_dayafter
        ,ot.oh_oo as IP_dayafter
        ,round(ot.outl,0) as OUTL_dayafter
        ,round(ot.units_above_outl,0) as excess_units_dayafter
        ,round(ot.units_above_outl - oy.units_above_outl,0) as DoD_excess_change
from orders o
join chewybi.products p using(product_part_number)
join chewybi.vendors v using(vendor_number)
left join aad.t_item_uom u
        on wh_id='AVP1'
        and p.product_part_number=u.item_number
left join sandbox_supply_chain.outl_excess_base oy
        on o.product_part_number=oy.product_part_number
        and oy.snapshot_date=o.document_order_date
left join sandbox_supply_chain.outl_excess_base ot
        on o.product_part_number=ot.product_part_number
        and ot.snapshot_date=o.document_order_date+1
left join regional_orders ro
        on o.product_part_number=ro.item_id
        and o.vendor_number=split_part(ro.supplier,'-',1)
        and o.location_code=ro.location_cd
where 1=1
        and coalesce(p.private_label_flag,false) is false
        and coalesce(v.vendor_direct_import_flag,false) is false
order by 7
;

select *
from sandbox_supply_chain.outl_excess_base
where 1=1
        and product_part_number='370875'
order by snapshot_date desc;