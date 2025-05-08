with source_data as (
    select * from {{ ref('stg_federated.0khpwlj09e2dr816ah3r31lyqh60') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        if category ='Demand' thenvalue
END as supply_(copy)_1600748244302979077,
        if category ='Supply' thenvalue
END as value_(copy)_1600748244302827524
    from source_data
)

select * from calculated_metrics