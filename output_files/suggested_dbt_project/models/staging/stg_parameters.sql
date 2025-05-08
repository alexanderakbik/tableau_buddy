with source as (
    select * from {{ source('parameters', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy), (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy)_1, (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy)_2, (br)_line-up_to_custom_(yyyy-mm)__(copy_2)_2146809667617992704, (br)_line-up_to_custom_(yyyy-mm)__(copy), (br)_line-up_to_custom_(yyyy-mm)__(copy)_1, (ru)_line-up_to_custom_(yyyy-mm)_)_(copy), (saf)_line-up_to_custom_(yyyy-mm)_(copy), (saf)_line-up_to_custom_(yyyy-mm)_(copy)_1, (saf)_line-up_to_custom_(yyyy-mm)_(copy)_2, (ser)_line-up_to_custom_(yyyy-mm)__(copy), (ser)_line-up_to_custom_(yyyy-mm)__(copy)_1, (us)_line-up_to_custom_(yyyy-mm)_(copy_2), (us)_line-up_to_custom_(yyyy-mm)_(copy_2)_1, (us)_line-up_to_custom_(yyyy-mm)_(copy_2)_2, (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888, (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888_1, (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888_2, (us)_line-up_to_custom_(yyyy-mm)_(copy), (us)_line-up_to_custom_(yyyy-mm)_(copy)_1, (us)_line-up_to_custom_(yyyy-mm)_(copy)_2, commodity_(copy), forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400, forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400_1, forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400_2, forecast_to_actual_(yyyy-mm)_(copy_2), forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529, forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529_1, forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529_2, forecast_to_actual_(yyyy-mm)_(copy), forecast_to_actual_(yyyy-mm)_(copy)_1, line-up_to_custom_(yyyy-mm)_(copy), line-up_to_custom_(yyyy-mm)_(copy)_1, line-up_to_custom_(yyyy-mm)_(copy)_2, parameter_1_1, parameter_2, parameter_3, parameter_4, parameter_8
    from source
)

select * from renamed