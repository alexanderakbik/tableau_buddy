with source as (
    select * from {{ source('federated.0alyknf0cdg0wm1bhvf9y1qpiczk', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        bbu, category, category_display_order, category_group_(group), data_source_(group), datasource, origin_group_(level_3,_eu), period_(yy/yy), quantity_dynamic_*, scenariolist, area, quality
    from source
)

select * from renamed