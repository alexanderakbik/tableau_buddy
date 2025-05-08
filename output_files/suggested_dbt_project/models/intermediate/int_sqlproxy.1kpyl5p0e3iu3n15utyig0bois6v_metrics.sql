with source_data as (
    select * from {{ ref('stg_sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        "2025-03" as (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888,
        "2025-03" as (us)_line-up_to_custom_(yyyy-mm)_(copy),
        "2025-03" as line-up_to_custom_(yyyy-mm)_(copy)_1,
        ZN(IF Calculation_804455546384973826='24/25'
THEN Actual * (copy)
ELSE 0
END) as act_22/23_(copy)_663155112171253760,
        Act 22/23 (copy)_663155112171253760+Proj 23/24 (copy)_257831087263092736 as calculation_1208653564136562688,
        IF Parameters.Commodity (copy) = "Soyoil" THEN Dest. Group (Level 2, Oils - SBO) (copy)_924082365985001472
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN Dest. Group (Level 2, Oils - PO)* (copy)_547187366510571521
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN Dest. Group (Level 2, Oils - RSO - EU) (copy)_924082365990588418
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN Dest. Group (Level 2, Oils - PO)* (copy)_547187366505148416
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN Dest. Group (Level 2, Oils - SFO - EU) (copy)_924082365990612995
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN Dest. Group (Level 2, Oils - PO)* (copy)_547187366513963011
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN Dest. Group (Level 2, Oils - PO - EU) (copy)_924082365990629380
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN Dest. Group (Level 3, EU) (copy)_880453736821772288
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN Dest. Group (Level 2, Soy Beans EU)
ELSEIF Parameters.Commodity (copy) = "Rapeseed" THEN Dest. Group (Level 2, Soy Beans/Meal) (copy)_794322424336855041
ELSEIF Parameters.Commodity (copy) = "Sunseeds" THEN Dest. Group (Level 2, Soy Beans EU)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN Dest. Group (Level 2, Soy Beans EU)
ELSEIF Parameters.Commodity (copy) = "Rapemeal" THEN Dest. Group (Level 2, Soy Beans EU)
ELSEIF Parameters.Commodity (copy) = "Sunmeal" THEN Dest. Group (Level 2, Soy Beans EU)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN Dest. Group (Level 2, Wheat) v2
ELSEIF Parameters.Commodity (copy) = "Corn" THEN Dest. Group (Level 2, Corn) (copy)_1762033393615564801
ELSEIF Parameters.Commodity (copy) = "Barley" THEN Dest. Group (Level 2, Corn) (copy)_1000080632292966401
END as dest._(level_2)_*_(copy)_1762033393615413248,
         as dest._group_(level_2,_corn)_(copy)_1762033393615564801,
        ZN({ FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='24/25'
THEN Projection * Corn (copy)_1108167023644725249
ELSE NULL
END)}) as proj_23/24_(copy)_257831087263092736,
        SUM(Exportable Surplus (Default) (copy)) - SUM(Calculation_608830385425960960) as surplus_/_deficit_(dynamic)_(copy_2)_1318991772893597697
    from source_data
)

select * from calculated_metrics