# Regional Ordering

## Purpose

This repository stores the SQL scripts that are used in the current Knime automated workflow. This repo will also contain all performance tracking scripts as well as compliance (adherence) to the regional ordering output recommedations.

## Technical Details of Regional Ordering Process

**TODO:** Add Technical document to this repo and add link to it here.

## Repository Structure

1. [Automated Process](https://github.com/cmorris10-chwy/Regional-Ordering/tree/main/Automated%20Process/SQL)
  - Directory contains the script(s) necessary to produce the daily regional ordering recommendations based on today's SO99 proposals to be released
3. [Compliance Tracking](https://github.com/cmorris10-chwy/Regional-Ordering/tree/main/Compliance%20Tracking)
  - Directory contains the script(s) necessary to track ordering adherence by Planner. This is meant to drive deeper discussions on gaps in the model's logic so it can become more accurate.
5. [KPI Metric Tracking](https://github.com/cmorris10-chwy/Regional-Ordering/tree/main/KPI%20Metric%20Tracking)
  - Directly contains the script(s) necessary to track the performance of this Regional Ordering process.
  - All KPI metrics will have their calculations stored here
7. [OUTL Excess Base Table](https://github.com/cmorris10-chwy/Regional-Ordering/tree/main/OUTL%20Excess%20Base%20Table)
  - This directly contains the link to the script that is used by BI in their daily batch runs. This script will update the data in this table based on today's inventory state and forecast.
  - Directly also contains script to be used when this table does not update correctly. An alert has been setup on RunMyQuery to send email to Cory Morris (PM) when the ETL process failed. This issue arises from EDW table update cadence and when the table is finished being updated.
