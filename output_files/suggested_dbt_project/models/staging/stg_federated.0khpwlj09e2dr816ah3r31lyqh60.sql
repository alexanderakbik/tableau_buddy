with source as (
    select * from {{ source('federated.0khpwlj09e2dr816ah3r31lyqh60', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        supply_(copy)_1600748244302979077, category, value, value_(copy)_1600748244302827524, year
    from source
)

select * from renamed