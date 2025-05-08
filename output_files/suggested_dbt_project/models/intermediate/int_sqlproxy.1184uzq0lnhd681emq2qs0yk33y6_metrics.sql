with source_data as (
    select * from {{ ref('stg_sqlproxy.1184uzq0lnhd681emq2qs0yk33y6') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        "R" as parameter_1_1,
        if Destination='Korea, South' then 'South Korea'
elseif Destination='United States' then 'United States of America'

ELSE Destination
END as calculation_1392175277239238660,
        IF Category = 'Imports' AND Attribute Name = 'TY Imp. from U.S.' THEN 'TY Imp. from U.S.' 
ELSEIF Category = 'Imports' AND Attribute Name = 'TY Imports' THEN 'TY Imports' 
ELSEIF Category = 'Exports' AND Attribute Name = 'TY Exports' THEN 'TY Exports' 
ELSEIF Commodity = 'Soybeans' AND Category = 'Production Crush' THEN 'Domestic Crush'

ELSE Category END as calculation_700872758527991827,
        IF Category = 'Imports' AND Attribute Name = 'Imports' 
THEN 'Shipment Export' ELSE Category END as calculation_700872758530551828,
        { MAX(Export Date Time (copy)_700872755466481664) } as calculation_700872758533533717,
        IF 
(ZN(SUM(Calculation_930274800807022599)) - LOOKUP(ZN(SUM(Calculation_930274800807022599)), -1))>0 THEN 'P'
ELSEIF (ZN(SUM(Calculation_930274800807022599)) - LOOKUP(ZN(SUM(Calculation_930274800807022599)), -1))=0 THEN 'Z'
ELSE 'N'
END as calculation_757167735445893120,
        IF Category Group = 'Area' THEN ROUND(Volume (mBu), 3) ELSE ROUND(Volume (mBu)) END as calculation_930274866357940224,
        DATE(Export_Date_Time) as export_date_time_(copy)_700872755466481664,
        IF Category Group = 'Area' THEN ROUND(Volume (000MT), 3) ELSE ROUND(Volume (000MT)) END as volume_(mbu)_round_(copy)_930274866395406337
    from source_data
)

select * from calculated_metrics