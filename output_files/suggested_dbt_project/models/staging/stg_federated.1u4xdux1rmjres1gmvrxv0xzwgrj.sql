with source as (
    select * from {{ source('federated.1u4xdux1rmjres1gmvrxv0xzwgrj', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        crop_year, ep, fas%, farmer_selling, month, origin, unsold
    from source
)

select * from renamed