with source_data as (
    select * from {{ ref('stg_federated.0alyknf0cdg0wm1bhvf9y1qpiczk') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
         as data_source_(group)
    from source_data
)

select * from calculated_metrics