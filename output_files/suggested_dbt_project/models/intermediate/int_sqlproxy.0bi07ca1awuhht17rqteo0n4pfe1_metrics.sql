with source_data as (
    select * from {{ ref('stg_sqlproxy.0bi07ca1awuhht17rqteo0n4pfe1') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        if left(REPORTTITLE, 3) = 'PSD'
then REPORTTITLE
end as calculation_1284088857746980865,
        if VERSION = {max (VERSION) }
then ((IMPORTS / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE)
END as calculation_1284088857770598404,
        IF SUM(Calculation_1284088857770598404) - sum (Max Imports  (copy)_1284088857771233285) <> 0 then SUM(Calculation_1284088857770598404) - sum (Max Imports  (copy)_1284088857771233285)  end as calculation_1284088857772126215,
        IFNULL(SUPPLY_TOTAL,
    IFNULL(CARRY_IN, 0) +
    IFNULL(IMPORTS,0) + 
    IFNULL(PRODUCTION,0)
) as calculation_269371557184516096,
        IFNULL(DOMESTIC_TOTAL,


    IFNULL(DOMESTIC_FEED_RESIDUAL,0) + 
    IFNULL(DOMESTIC_ETHANOL,0) + 
    IFNULL(DOMESTIC_FSI,0) + 
    IFNULL(DOMESTIC_FOOD,0) + 
    IFNULL(DOMESTIC_SEED,0)    
)
+
IFNULL(EXPORTS,0) as calculation_269371557184688129,
        Calculation_269371557184516096 - Calculation_269371557184688129 as calculation_269371558547099650,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(CARRY_IN)
ELSE
    SUM(
        (CARRY_IN / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227002883,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(CARRY_OUT)
ELSE
    SUM(
        (CARRY_OUT / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227158532,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_TOTAL)
ELSE
    SUM(
        (DOMESTIC_TOTAL / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227265029,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_ETHANOL)
ELSE
    SUM(
        (DOMESTIC_ETHANOL / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227441158,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_FEED_RESIDUAL)
ELSE
    SUM(
        (DOMESTIC_FEED_RESIDUAL / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227551751,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_FOOD)
ELSE
    SUM(
        (DOMESTIC_FOOD / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227666440,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_FSI)
ELSE
    SUM(
        (DOMESTIC_FSI / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227752457,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(DOMESTIC_SEED)
ELSE
    SUM(
        (DOMESTIC_SEED / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559227912202,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(HARVESTED_AREA)
ELSE
    SUM(
        (HARVESTED_AREA / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228018699,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(IMPORTS)
ELSE
    SUM(
        (IMPORTS / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228190732,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(PLANTED_AREA)
ELSE
    SUM(
        (PLANTED_AREA / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228289037,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(PRODUCTION)
ELSE
    SUM(
        (PRODUCTION / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228370958,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(SUPPLY_TOTAL)
ELSE
    SUM(
        (SUPPLY_TOTAL / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228440591,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    AVG(YIELD)
ELSE
    AVG(
        (YIELD / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559228583952,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(Calculation_269371557184516096)
ELSE
    SUM(
        (Calculation_269371557184516096 / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559241658386,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(Calculation_269371557184688129)
ELSE
    SUM(
        (Calculation_269371557184688129 / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559241768979,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(Calculation_269371558547099650)
ELSE
    SUM(
        (Calculation_269371558547099650 / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_269371559241895956,
        if START_MONTH_MARKETING_YEAR > 6 then Calculation_315814937908121601 END as calculation_315814937908072448,
        if MONTH(RELEASEDATE) < START_MONTH_MARKETING_YEAR
then STR(YEAR(RELEASEDATE)-1)+'/'+RIGHT(STR(YEAR(RELEASEDATE)),2)
else STR(YEAR(RELEASEDATE))+'/'+RIGHT(STR(YEAR(RELEASEDATE)+1),2)
end as calculation_315814937908121601,
        IF    
    REPORTTITLE IN Reporttitle Set
//Releasedate = {max(Releasedate)}
    AND WASDE_REPORT = {MAX(WASDE_REPORT)}
    AND ISO_A2_CODE = 'US'
//AND Region in Included regions
THEN
    IF START_MONTH_MARKETING_YEAR > 6 THEN     
        IF MARKET_YEAR = {INCLUDE COMMODITY:MAX(Calculation_315814937908072448)}
            THEN Exports in display unit (copy)_315814937909039107 
        END
    ELSE IF  MARKET_YEAR = {INCLUDE COMMODITY:MAX(Calculation_315814937908121601)} 
            THEN Exports in display unit (copy)_315814937909039107 
        END
    END
END as calculation_315814937908301826,
        if (
    TOTAL( 
        COUNTD( UNIT_CODE )
    ) = 
    SUM(
        {fixed : COUNTD( UNIT_CODE )} )
    ) 
then 
    sum(EXPORTS)
ELSE
    SUM(
        (EXPORTS / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE 
    )
END as calculation_425027274611560449,
         as country_name_(region),
        (EXPORTS / DIVISION_TO_BASE_UNIT) * MULTIPLY_VALUE_FROM_BASE as exports_in_display_unit_(copy)_315814937909039107,
        SUM(sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v.Projection * Corn (copy)_1108167023644725249)-SUM(Calculation_1284088857770598404)/1000 as max_imports__(copy)_1188950320941858817,
        if (VERSION) <> {max (VERSION)}
then ((IMPORTS/DIVISION_TO_BASE_UNIT)*MULTIPLY_VALUE_FROM_BASE)
END as max_imports__(copy)_1284088857771233285,
        TRIM( SPLIT( SPLIT( SPLIT( SPLIT( REPORTTITLE, "PSD", 2 ), "Version", 2 ), ":", 2 ), ".", 1 ) ) as reporttitle_-_split_1,
        TRIM( SPLIT( SPLIT( SPLIT( SPLIT( SPLIT( REPORTTITLE, "PSD", 2 ), "Version", 2 ), ":", 3 ), "psd", 2 ), "_", 2 ) ) as reporttitle_-_split_2
    from source_data
)

select * from calculated_metrics