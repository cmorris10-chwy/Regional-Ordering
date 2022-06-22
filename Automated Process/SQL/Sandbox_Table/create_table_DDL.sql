drop table if exists sandbox_supply_chain.regional_ordering;
create table sandbox_supply_chain.regional_ordering as
        (
                snapshot_date date
                ,supply_planner varchar(20)
                ,MC1 varchar(20)
                ,action_ varchar(30)
                ,item varchar(7)
                ,region varchar(12)
                ,location varchar(4)
                ,product_abc_code varchar(1)
                ,private_label_flag boolean
                ,is_NEW_ITEM boolean
                ,avg_daily_forecast numeric
                ,supplier varchar(20)
                ,vendor_name varchar
                ,vendor_distribution_method varchar(25)
                ,vendor_direct_import_flag boolean
                ,release_date date
                ,ERDD date
                ,MOQ int
                ,projected_region_oos boolean
                ,FC_NEED_so99 numeric
                ,proposed_FC_qty int
                ,FC_Region_Need_percent_so99 numeric
                ,region_proposed_cummulative_quantity int
                ,total_region_SS numeric
                ,total_region_IP numeric
                ,total_region_NEED numeric
                ,fc_need_rank_in_region int
                ,region_NEED_left_after_ordering_cummulative numeric
                ,item_MOQ_more_than_60DOS boolean
        )
;