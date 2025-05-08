with source_data as (
    select * from {{ ref('stg_federated.069o6s01orkucn19yfgu619o0stu') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        Value/{ FIXED Origin,Destination:SUM(Forecasted Demand)} as coverage_ratio_(copy_2)_1663235650131365893,
        { FIXED Month,Destination:AVG(Forecasted Demand)} as forecasted_demand_(chart)_(copy)_2016486762009780224,
        { FIXED Destination,Origin,Month (group):SUM(Forecasted Demand)} as forecasted_demand_(chart)_(copy)_926897118486876160,
        AVG(Forecasted Demand)-SUM(Value) as forecasted_demand_(copy)_1709679020567498753,
        { FIXED Destination,Origin:SUM(Forecasted Demand)} as forecasted_demand_(copy)_1891230386964115456,
         as month_(group),
        { FIXED Destination,Month,Crop year:AVG(Forecasted Demand)}-{ FIXED Destination,Month,Crop year:SUM(Value)} as open_demand_(copy)_1663235650134941703,
        SUM(Value)/AVG(Forecasted Demand) as open_demand_(copy)_1709679020568346626,
        SUM(Forecasted Demand (chart) (copy)_2016486762009780224)-SUM(Value) as open_demand_(copy)_2016486762012291073
    from source_data
)

select * from calculated_metrics