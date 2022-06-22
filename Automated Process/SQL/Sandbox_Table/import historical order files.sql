drop table if exists historical_files;
create local temp table historical_files
        (
                snapshot_date date
--                ,supply_planner varchar(20)
--                ,MC1 varchar(20)
                ,action_ varchar(30)
                ,item varchar(7)
--                ,region varchar(12)
--                ,location varchar(4)
--                ,product_abc_code varchar(1)
--                ,is_NEW_ITEM boolean
--                ,avg_daily_forecast numeric
                ,supplier varchar(20)
--                ,vendor_name varchar
--                ,vendor_distribution_method varchar(25)
--                ,release_date date
--                ,ERDD date
--                ,MOQ int
--                ,projected_region_oos boolean
--                ,FC_NEED_so99 numeric
                ,proposed_FC_qty int
                ,FC_Region_Need_percent_so99 numeric
--                ,region_proposed_cummulative_quantity int
--                ,total_region_SS numeric
--                ,total_region_IP numeric
--                ,total_region_NEED numeric
--                ,fc_need_rank_in_region int
--                ,region_NEED_left_after_ordering_cummulative numeric
--                ,item_MOQ_more_than_60DOS boolean
        )
on commit preserve rows;
copy historical_files
from local 'C:\Users\cmorris10\Downloads\Regional Order Import Files\6_15_22_ordersv2.csv'
parser fcsvparser(delimiter = ',')
;
--add in PL and DI flags before inserting into sandbox

--drop table if exists sandbox_supply_chain.cmorris10_history_data_test;
--create table sandbox_supply_chain.cmorris10_history_data_test
--        (
--                order_date date
--                ,action_ varchar(30)
--                ,item varchar(7)
--                ,supplier varchar(20)
--                ,proposed_FC_qty int
--                ,FC_Region_Need_percent_so99 numeric
--        )
--;

INSERT /*direct*/ INTO sandbox_supply_chain.cmorris10_history_data_test
        select h.*
        from historical_files h
        join chewybi.products p on h.item=p.product_part_number
                                and coalesce(p.private_label_flag,false) is false
        join chewybi.vendors v on split_part(h.supplier,'-',1)=v.vendor_number
                                and coalesce(v.vendor_direct_import_flag,false) is false
;

--select distinct order_date from sandbox_supply_chain.cmorris10_history_data_test;