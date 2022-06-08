-- This query is used to view the data in the table such as DoD SS totals.
select snapshot_date, sum(SS) as SS from sandbox_supply_chain.outl_excess_base group by 1 order by 1 desc;

-- Run below lines to delete and update problematic data
DELETE /*+direct*/ FROM sandbox_supply_chain.outl_excess_base where snapshot_date in ('2022-05-29','2022-05-28') --User to update the list of dates for which data is to be removed.
INSERT /*+direct*/ INTO sandbox_supply_chain.outl_excess_base (
-- For each date that needs data you will need to add a Union of the query on lines 8-31. Update the date to input as the snapshot_date while keeping everything else the same.
select '2022-05-29'::date as snapshot_date 
        ,product_part_number
        , avg_daily_forecast
        , oh
        , ss
        , fcast_over_lt_rp
        , oo
        , outl
        , oh_oo
        , units_above_outl
        , days_above_outl
        , parent_company
        , private_label_flag
        , product_name -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , product_merch_classification1
        , product_merch_classification2 -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , product_merch_classification3 -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , otb_flag
        , product_abc_code
        , units_aged_over_60_days
        , excess_inventory
        , reason_for_excess
from sandbox_supply_chain.outl_excess_base
where snapshot_date='2022-05-28'
union
select '2022-05-30'::date as snapshot_date
        ,product_part_number
        , avg_daily_forecast
        , oh
        , ss
        , fcast_over_lt_rp
        , oo
        , outl
        , oh_oo
        , units_above_outl
        , days_above_outl
        , parent_company
        , private_label_flag
        , product_name -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , product_merch_classification1
        , product_merch_classification2 -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , product_merch_classification3 -- removing as this information is not needed but we do not want to lose history data by creating a new table.
        , otb_flag
        , product_abc_code
        , units_aged_over_60_days
        , excess_inventory
        , reason_for_excess
from sandbox_supply_chain.outl_excess_base
where snapshot_date='2022-05-28'
);