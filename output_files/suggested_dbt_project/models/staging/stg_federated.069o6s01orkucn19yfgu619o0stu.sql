with source as (
    select * from {{ source('federated.069o6s01orkucn19yfgu619o0stu', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        :measure_names, coverage_ratio_(copy_2)_1663235650131365893, crop_year, destination, destination_(group), f7, forecasted_demand, forecasted_demand_(chart)_(copy)_2016486762009780224, forecasted_demand_(chart)_(copy)_926897118486876160, forecasted_demand_(copy)_1709679020567498753, forecasted_demand_(copy)_1891230386964115456, month, month_(group), open_demand_(copy)_1663235650134941703, open_demand_(copy)_1709679020568346626, open_demand_(copy)_2016486762012291073, origin, value
    from source
)

select * from renamed