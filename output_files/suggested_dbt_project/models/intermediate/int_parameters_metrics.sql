with source_data as (
    select * from {{ ref('stg_parameters') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        "2023-02" as (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy)_1,
        "2023-10" as (br)_line-up_to_custom_(yyyy-mm)__(copy_2)_2146809667617992704,
        "2023-03" as (br)_line-up_to_custom_(yyyy-mm)__(copy)_1,
        "2023-01" as (saf)_line-up_to_custom_(yyyy-mm)_(copy)_1,
        "2023-01" as (us)_line-up_to_custom_(yyyy-mm)_(copy_2)_1,
        "2022-11" as forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400_1,
        "2022-11" as forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529_1,
        "2022-06" as forecast_to_actual_(yyyy-mm)_(copy)_1,
        9 as parameter_2,
        "Month" as parameter_3,
        "Corn" as parameter_4,
        "2025-03" as parameter_8
    from source_data
)

select * from calculated_metrics