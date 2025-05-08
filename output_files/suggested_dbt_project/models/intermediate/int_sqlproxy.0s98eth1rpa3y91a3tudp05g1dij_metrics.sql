with source_data as (
    select * from {{ ref('stg_sqlproxy.0s98eth1rpa3y91a3tudp05g1dij') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        IFNULL(GrainUse,'Any usage') as calculation_269934509093781504,
        IFNULL(GrainUse,'Any usage') as calculation_269934509095219203,
        str(year (LoadDate)) + "-" + 
if str(month(LoadDate)) = "1" then "01" 
elseif str(month(LoadDate)) = "2" then "02"
elseif str(month(LoadDate)) = "3" then "03"
elseif str(month(LoadDate)) = "4" then "04"
elseif str(month(LoadDate)) = "5" then "05"
elseif str(month(LoadDate)) = "6" then "06"
elseif str(month(LoadDate)) = "7" then "07"
elseif str(month(LoadDate)) = "8" then "08"
elseif str(month(LoadDate)) = "9" then "09"

else str(month(LoadDate))
END as calculation_307370685664710656,
         as commodity_origin_name_(wb)_(group),
         as commodity_segment_name_(mapped),
        IFNULL(GrainUse,'Any type') as commodity_usage_name_(wb)_(copy),
        str(year (LoadDate)) as crop_calendar_date_yyyy_(wb)_(copy),
        str(year (LoadDate)) + "-" + 
if str(month(LoadDate)) = "1" then "01" 
elseif str(month(LoadDate)) = "2" then "02"
elseif str(month(LoadDate)) = "3" then "03"
elseif str(month(LoadDate)) = "4" then "04"
elseif str(month(LoadDate)) = "5" then "05"
elseif str(month(LoadDate)) = "6" then "06"
elseif str(month(LoadDate)) = "7" then "07"
elseif str(month(LoadDate)) = "8" then "08"
elseif str(month(LoadDate)) = "9" then "09"

else str(month(LoadDate))
END as crop_calendar_date_yyyy-mm_(wb)_(copy),
        (IF MONTH(LoadDate)>=9 then str(year(LoadDate))
else str(year(LoadDate)-1)
END) as cropyear_(copy),
         as dischcountry_(group),
         as discharge_area,
         as loadberthterminalport_(group)_1,
         as loadberthterminalport_(group)
    from source_data
)

select * from calculated_metrics