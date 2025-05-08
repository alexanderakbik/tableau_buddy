with source_data as (
    select * from {{ ref('stg_sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v_(copy)') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        "2025-03" as (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy),
        "2025-03" as (br)_line-up_to_custom_(yyyy-mm)__(copy),
        "2025-03" as (ru)_line-up_to_custom_(yyyy-mm)_)_(copy),
        "2025-03" as (saf)_line-up_to_custom_(yyyy-mm)_(copy),
        "2025-03" as (ser)_line-up_to_custom_(yyyy-mm)__(copy),
        "2025-03" as (us)_line-up_to_custom_(yyyy-mm)_(copy_2),
        "2023-06" as (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888_1,
        "2022-08" as (us)_line-up_to_custom_(yyyy-mm)_(copy)_1,
        "Corn" as commodity_(copy),
        "2025-03" as forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400,
        "Exclusive" as forecast_to_actual_(yyyy-mm)_(copy_2),
        "2025-03" as forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529,
        "2022-06" as forecast_to_actual_(yyyy-mm)_(copy),
        "2025-03" as line-up_to_custom_(yyyy-mm)_(copy),
        IF Parameters.Parameter 1 1 = 'R' THEN Volume (mBu) Round (copy)_930274866395406337 
ELSEIF Parameters.Parameter 1 1 = 'D' THEN Calculation_930274866357940224
END as calculation_930274800807022599,
         as destination_(group),
        1 as number_of_records,
        IF Calculation_215046905796349953='European Union' and Dynamic Period (YYYY) * (copy) > Parameters.Forecast to Actual (YYYY-MM) (copy 3)_790944748003606529 THEN Calculation_625155928475000833
ELSEIF Calculation_215046905796349953='European Union' and Dynamic Period (YYYY) * (copy) <= Parameters.Forecast to Actual (YYYY-MM) (copy 3)_790944748003606529 THEN Actual * (copy)
ELSEIF Calculation_215046905796349953<>'European Union' THEN Projection * (copy)

END as projection_*_corn_(copy)_1108167023644725249,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='19/20'
THEN Projection * (copy)
ELSE NULL
END)} as 18/19_(copy),
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='20/21'
THEN Projection * (copy)
ELSE NULL
END)} as 19/20_(copy)_1372472030682869761,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='21/22'
THEN Line-up * (copy)_1816920976333086721
ELSE NULL
END)} as 20/21_(copy)_1274518700892217344,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='21/22'
THEN Projection * (copy)
ELSE NULL
END)} as 20/21_(copy)_1949495690456383489,
        IF Calculation_804455546384973826='21/22'
THEN Actual * (copy)
ELSE NULL
END as act_(copy)_1053842374137585667,
        IF Calculation_804455546384973826='22/23'
THEN Actual * (copy)
ELSE NULL
END as act_21/22_(copy)_338332926771908608,
        IF Parameters.Forecast to Actual (YYYY-MM) (copy 2) = 'Inclusive' 
    THEN Quantity (Custom) * (copy) 
    ELSE Quantity (ExIR, Custom) * (copy) 
END as actual_(shipment)_*_(copy),
        IF (Calculation_795729827018858500='United States of America')
AND Dynamic Period (YYYY) * (copy) > Parameters.(BR) Line-up to Custom (YYYY-MM)  (copy)
THEN Actual (Shipment) * (copy) 

ELSEIF (Calculation_795729827018858500='European Union')
AND Dynamic Period (YYYY) * (copy) > Parameters.(US) Line-up to Custom (YYYY-MM) (copy) 1
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Argentina')
AND Dynamic Period (YYYY) * (copy) > Parameters.Line-up to Custom (YYYY-MM) (copy)
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Brazil')
AND Dynamic Period (YYYY) * (copy) > Parameters.(AR) Line-up to Custom (YYYY-MM) (copy) (copy)
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Canada')
AND Dynamic Period (YYYY) * (copy) > Parameters.(US) Line-up to Custom (YYYY-MM) (copy 3)_1740359830515621888 1
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Ukraine')
AND Dynamic Period (YYYY) * (copy) > Parameters.(RU) Line-up to Custom (YYYY-MM) ) (copy)
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Russia')
AND Dynamic Period (YYYY) * (copy) > Parameters.(SER) Line-up to Custom (YYYY-MM)  (copy)
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='Serbia')
AND Dynamic Period (YYYY) * (copy) > Parameters.(SAF) Line-up to Custom (YYYY-MM) (copy)
THEN Actual (Shipment) * (copy)

ELSEIF (Calculation_795729827018858500='South Africa')
AND Dynamic Period (YYYY) * (copy) > Parameters.(US) Line-up to Custom (YYYY-MM) (copy 2)
THEN Actual (Shipment) * (copy)

ELSE Line-up (Shipment) * (copy)

END
//AND ISNULL({ FIXED Item,Dynamic Period (YYYY-MM) *,Origin (Level 3),Dest. (Level 4):SUM(Custom *)}) as actual_*_(copy),
        IF DataType = 'GLENCORE-CUSTOM' 
    THEN Calculation_843017618144940032 
    ELSE NULL 
END as calculation_1038079781623758848,
        IF CONTAINS(Commodity,'Equivalent') 
    THEN 'Y' 
    ELSE 'N' 
END as calculation_1066790170010402816,
        { FIXED Calculation_804455546384973826, AsOfDate, Calculation_1146447644138991618,Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='18/19'
THEN Quantity (Projection, Default) * (copy)
ELSE NULL
END)} as calculation_1099441275459854338,
        IF Calculation_804455546384973826='20/21'
THEN Actual * (copy)
ELSE NULL
END as calculation_1099441275466203139,
        IF Calculation_804455546384973826='19/20'
THEN Projection * (copy)
ELSE NULL
END as calculation_1099441275466469380,
        Origin as calculation_1141381097684979712,
        LEFT(Calculation_130041443275886594,2) as calculation_1141381097720352769,
        IF     Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=1 THEN "Q4-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=2 THEN "Q4-NDJ"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=3 THEN "Q4-DJF"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=4 THEN "Q3-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=5 THEN "Q3-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=6 THEN "Q2-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=7 THEN "Q2-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=8 THEN "Q2-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=9 THEN "Q1-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=10 THEN "Q1-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=11 THEN "Q1-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2 3=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2 3=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2 3=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2 3=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2 3=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2 3=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2 3=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2 3=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2 3=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2 3=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2 3=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2 3=12 THEN "Q4-SON"
END as calculation_1146447644122546176,
        IF Parameters.Parameter 3 1 = "Quarter" 
    THEN Calculation_1141381097720352769
ELSEIF Parameters.Parameter 3 1 = "Month" 
    THEN Period (YYYY-MM) (copy)
END as calculation_1146447644136132609,
        LEFT(Calculation_1146447644122546176,2) as calculation_1146447644138991618,
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
ELSEIF Parameters.Commodity (copy) = "Corn" THEN Dest. Group (Level 2, Corn) v2
ELSEIF Parameters.Commodity (copy) = "Barley" THEN Dest. Group (Level 2, Corn) (copy)_1000080632292966401
END as calculation_1146447644456341507,
        IF Parameters.Commodity (copy) = "Soyoil" THEN Dest. Group (Level 1, Oils - SBO - EU) (copy)_239535223290937348
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN Dest. Group (Level 1, Oils - SBO)*
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN Dest. Group (Level 1, Oils - RSO - EU) (copy)_239535223290916867
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN Dest. Group (Level 1, Oils - RSO)* 
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN Dest. Group (Level 1, Oils - SFO - EU) (copy)_239535223290957829
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN Dest. Group (Level 1, Oils - SFO)*
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN Dest. Group (Level 1, Oils - PO - EU) (copy)_239535223290892290
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN Dest. Group (Level 1, Oils - PO)*
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN Dest. Group (Level 1, Soy Beans EU) (group)
ELSEIF Parameters.Commodity (copy) = "Rapeseed" THEN Dest. Group (Level 1, Soy Beans/Meal) (copy)_794322424336744448
ELSEIF Parameters.Commodity (copy) = "Sunseeds" THEN Dest. Group (Level 1, Soy Beans EU) (group)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN Dest. Group (Level 1, Soy Beans EU) (group)
ELSEIF Parameters.Commodity (copy) = "Rapemeal" THEN Dest. Group (Level 1, Soy Beans EU) (group)
ELSEIF Parameters.Commodity (copy) = "Sunmeal" THEN Dest. Group (Level 1, Soy Beans EU) (group)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN Dest. Group (Level 1, Wheat) v2
ELSEIF Parameters.Commodity (copy) = "Corn" THEN Dest. Group (Level 1, Corn) v2
ELSEIF Parameters.Commodity (copy) = "Barley" THEN Dest. Group (Level 1, Corn) (copy)_1000080632292859904
END as calculation_1146447644456574980,
        ISOWEEK(Period) as calculation_1273111365430665216,
        'Week '+STR(Calculation_1273111365430665216) as calculation_1273111365430857729,
        IF Period (YYYY-MM) (copy) > Parameters.Parameter 8 1 
  THEN 'Forecast'
ELSEIF Period (YYYY-MM) (copy) > Parameters.Forecast to Actual (YYYY-MM) (copy)
  THEN 'Line-up'
  ELSE 'Custom' 
END as calculation_130041442935742464,
        Commodity 
+ IFNULL(' ' + Usage, '')
+ IFNULL(' ' + Grade, '') 
+ IFNULL(' ' + Item , '') as calculation_130041442962415617,
        IF     Period (Mmm) (copy 2)="Jan"  AND CropMonth=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=1 THEN "Q4-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=2 THEN "Q4-NDJ"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=3 THEN "Q4-DJF"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=4 THEN "Q3-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=5 THEN "Q3-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=6 THEN "Q2-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=7 THEN "Q2-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=8 THEN "Q2-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=9 THEN "Q1-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=10 THEN "Q1-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=11 THEN "Q1-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND CropMonth=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND CropMonth=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND CropMonth=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND CropMonth=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND CropMonth=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND CropMonth=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND CropMonth=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND CropMonth=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND CropMonth=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND CropMonth=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND CropMonth=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND CropMonth=12 THEN "Q4-SON"
END as calculation_130041443275886594,
        IF Commodity = 'Soybeans' AND
Category (copy) = 'Domestic Residual' 
OR Commodity = 'Soybeans' AND
Category (copy) = 'Domestic Seed/Feed'
THEN 'Domestic Feed / Res' 
ELSE Category (copy)
END as calculation_1392175277222854656,
        SUM(Proj 20/21 (copy)_1372472030682599424)-SUM(sqlproxy.1184uzq0lnhd681emq2qs0yk33y6.Calculation_930274800807022599) as calculation_1392175277234503683,
        Calculation_924082365988470785 = Commodity as calculation_181269907789590528,
        SUM(Act 21/22 (copy)_338332926771908608)-SUM(Proj 21/22 (copy)_1949495690455158784) as calculation_1816920976331595776,
        IF Parameters.Commodity (copy) = "Soyoil" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN Origin Group (Level 3, Soy Meal) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN Origin Group (Level 3,Wheat)  (copy 2)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN Origin Group (Level 3, Soy Beans) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN Origin Group (Level 3, EU) (copy)
ELSEIF Parameters.Commodity (copy) = "Corn" THEN Origin Group (Level 3, Oils) (copy 2)
END as calculation_215046905796349953,
        CASE Category (copy)
    WHEN 'Carry In'       THEN Carry In (Default) (copy 4)
    WHEN 'Carry Out'      THEN Carry Out (Default) (copy 4)
    WHEN 'Govt Carry In'  THEN Govt Carry In (Default) (copy)
    WHEN 'Govt Carry Out' THEN Govt Carry Out (Default) (copy)
    ELSE QtyDefaultUnit
END as calculation_215046905820536834,
        SUM(Carry In (copy)) / SUM(Calculation_608830385777811459) as calculation_347058659960877056,
        IF Category (copy) = 'Carry In' THEN 'Supply'
ELSE Category Group (copy)
END as calculation_475692731735433216,
        SUM(Carry In (Default) (copy)) - SUM(Carry Out (Default) (copy)) as calculation_475692731757924355,
        IF Period (YYYY-MM) (copy) > Parameters.Parameter 8 1 
    THEN Calculation_625155928475000833
    ELSE Quantity (Projection, Default) * (copy)
END as calculation_608830385425960960,
        IF Category (copy) = 'Exports'
    THEN Quantity Dynamic with Aggregations * (copy)
    ELSE NULL
END as calculation_608830385776943105,
        SUM(Calculation_608830385776943105) - SUM(Calculation_608830385425960960) as calculation_608830385777397762,
        IF Category Group (copy) = 'Demand'
    THEN Quantity Dynamic with Aggregations * (copy)
    ELSE NULL
END as calculation_608830385777811459,
        IF Parameters.Forecast to Actual (YYYY-MM) (copy 2) = 'Inclusive' 
    THEN Quantity (Line-up) * (copy) 
    ELSE Quantity (ExIR, Line-up) * (copy) 
END as calculation_625155928475000833,
        SUM(Calculation_608830385425960960) - SUM(Quantity (Projection, Default) * (copy)) as calculation_702843024510869508,
        IF (Origin (EU together) = Destination (IntraRegional Groups))
    THEN NULL
    ELSE Calculation_1038079781623758848 
END as calculation_795729826980593664,
        Destination (IntraRegional Groups) as calculation_795729827015913473,
        STR(Destination) as calculation_795729827016142850,
        Origin (EU together) as calculation_795729827018858500,
        IF INT(Period (MM) (copy)) < Parameters.Parameter 2 3
    THEN RIGHT("0" + STR(IF INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 = -1 
                            THEN 99 
                            ELSE INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 
                         END),2)
         + "/" + RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
ELSEIF  INT(Period (MM) (copy))>= Parameters.Parameter 2 3
    THEN RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
         + "/" + RIGHT("0"+ STR(INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))+1),2)
END as calculation_804455546384973826,
        YEAR(Period) as calculation_804455546385244163,
        MONTH(Period) as calculation_804455546385559556,
        IF Parameters.Parameter 1 1 1 = 'D' 
    THEN QtyDefaultUnit
ELSEIF Parameters.Parameter 1 1 1 = 'R' 
    THEN QtyReportingUnit
END as calculation_843017618144940032,
        IF Parameters.Parameter 1 1 1 = 'D' 
    THEN DefaultUnit
ELSEIF Parameters.Parameter 1 1 1 = 'R' 
    THEN ReportingUnit
END as calculation_843017618145730561,
        IF Parameters.Commodity (copy) = 'Sunoil EU' THEN 'Sunoil'
ELSEIF Parameters.Commodity (copy) = 'Soyoil EU' THEN 'Soyoil'
ELSEIF Parameters.Commodity (copy) = 'Rapeoil EU' THEN 'Rapeoil'
ELSEIF Parameters.Commodity (copy) = 'Palmoil EU' THEN 'Palmoil'
ELSE Parameters.Commodity (copy)
END as calculation_924082365988470785,
        IF Category (copy) = 'Production'
    THEN Quantity Dynamic with Aggregations * (copy)
    ELSE NULL
END as calculation_975592303034843136,
        IF Category (copy) = 'Imports'
    THEN Quantity Dynamic with Aggregations * (copy)
    ELSE NULL
END as calculation_975592303035179009,
        IF Category (copy) = 'Production'
    THEN Quantity * (copy 2)
    ELSE NULL
END as calculation_975592303035445250,
        IF Category (copy) = 'Imports'
    THEN Quantity * (copy 2)
    ELSE NULL
END as calculation_975592303035588611,
        Commodity as calculation_975592304884084740,
        IF Category (copy) = 'Govt Carry In' 
AND CropMonthSeq = 'M01'
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_(default)_(copy_2),
        IF Category (copy) = 'Carry In' 
AND    (CropMonthSeq = 'M01'
     OR CropMonthSeq = 'M04'
     OR CropMonthSeq = 'M07'
     OR CropMonthSeq = 'M10')
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_(default)_(copy_3),
        IF Category (copy) = 'Carry In' 
AND CropMonthSeq = 'M01'
    THEN QtyDefaultUnit
    ELSE NULL
END as carry_in_(default)_(copy_4),
        IF Category (copy) = 'Carry In' 
AND MONTH(Period) = Parameters.Parameter 2 3
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_(default)_(copy),
        IF Category (copy) = 'Carry In' 
AND (MONTH(Period) = Parameters.Parameter 2 3
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 4
         WHEN 2  THEN 5
         WHEN 3  THEN 6
         WHEN 4  THEN 7
         WHEN 5  THEN 8
         WHEN 6  THEN 9
         WHEN 7  THEN 10
         WHEN 8  THEN 11
         WHEN 9  THEN 12
         WHEN 10 THEN 1
         WHEN 11 THEN 2
         WHEN 12 THEN 3
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 7
         WHEN 2  THEN 8
         WHEN 3  THEN 9
         WHEN 4  THEN 10
         WHEN 5  THEN 11
         WHEN 6  THEN 12
         WHEN 7  THEN 1
         WHEN 8  THEN 2
         WHEN 9  THEN 3
         WHEN 10 THEN 4
         WHEN 11 THEN 5
         WHEN 12 THEN 6
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 10
         WHEN 2  THEN 11
         WHEN 3  THEN 12
         WHEN 4  THEN 1
         WHEN 5  THEN 2
         WHEN 6  THEN 3
         WHEN 7  THEN 4
         WHEN 8  THEN 5
         WHEN 9  THEN 6
         WHEN 10 THEN 7
         WHEN 11 THEN 8
         WHEN 12 THEN 9
     END)
     THEN Quantity (SND) * (copy)
     ELSE NULL
END as carry_in_(dynamic)_(copy_2),
        IF Category (copy) = 'Govt Carry In' 
AND MONTH(Period) = Parameters.Parameter 2 3
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_(dynamic)_(copy),
        IF Category (copy) = 'Carry Out' 
AND CropMonthSeq = 'M12'
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_(copy),
        SUM(Quantity * (copy)) - SUM(Carry In (copy)) as carry_in_-_carry_out_(dynamic)_*_(copy),
        IF Category (copy) = 'Carry In' 
AND    (CropMonthSeq = 'M01'
     OR CropMonthSeq = 'M04'
     OR CropMonthSeq = 'M07'
     OR CropMonthSeq = 'M10')
    THEN QtyDefaultUnit
    ELSE NULL
END as carry_in_per_quarter_(default)_(copy_2),
        IF Category (copy) = 'Govt Carry In' 
AND    (CropMonthSeq = 'M01'
     OR CropMonthSeq = 'M04'
     OR CropMonthSeq = 'M07'
     OR CropMonthSeq = 'M10')
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_in_per_quarter_(default)_(copy),
        IF Category (copy) = 'Govt Carry Out' 
AND CropMonthSeq = 'M12'
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_out_(default)_(copy_2),
        IF Category (copy) = 'Govt Carry Out' 
AND    (CropMonthSeq = 'M03'
     OR CropMonthSeq = 'M06'
     OR CropMonthSeq = 'M09'
     OR CropMonthSeq = 'M12')
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_out_(default)_(copy_3),
        IF Category (copy) = 'Carry Out' 
AND CropMonthSeq = 'M12'
    THEN QtyDefaultUnit
    ELSE NULL
END as carry_out_(default)_(copy_4),
        IF Category (copy) = 'Carry Out' 
AND MONTH(Period) = 
    CASE Parameters.Parameter 2 3
        WHEN 1  THEN 12
        WHEN 2  THEN 1
        WHEN 3  THEN 2
        WHEN 4  THEN 3
        WHEN 5  THEN 4
        WHEN 6  THEN 5
        WHEN 7  THEN 6
        WHEN 8  THEN 7
        WHEN 9  THEN 8
        WHEN 10 THEN 9
        WHEN 11 THEN 10
        WHEN 12 THEN 11
    END
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_out_(default)_(copy),
        IF Category (copy) = 'Carry Out' 
AND (MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 12
         WHEN 2  THEN 1
         WHEN 3  THEN 2
         WHEN 4  THEN 3
         WHEN 5  THEN 4
         WHEN 6  THEN 5
         WHEN 7  THEN 6
         WHEN 8  THEN 7
         WHEN 9  THEN 8
         WHEN 10 THEN 9
         WHEN 11 THEN 10
         WHEN 12 THEN 11
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 9
         WHEN 2  THEN 10
         WHEN 3  THEN 11
         WHEN 4  THEN 12
         WHEN 5  THEN 1
         WHEN 6  THEN 2
         WHEN 7  THEN 3
         WHEN 8  THEN 4
         WHEN 9  THEN 5
         WHEN 10 THEN 6
         WHEN 11 THEN 7
         WHEN 12 THEN 8
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 6
         WHEN 2  THEN 7
         WHEN 3  THEN 8
         WHEN 4  THEN 9
         WHEN 5  THEN 10
         WHEN 6  THEN 11
         WHEN 7  THEN 12
         WHEN 8  THEN 1
         WHEN 9  THEN 2
         WHEN 10 THEN 3
         WHEN 11 THEN 4
         WHEN 12 THEN 5
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 3
         WHEN 2  THEN 4
         WHEN 3  THEN 5
         WHEN 4  THEN 6
         WHEN 5  THEN 7
         WHEN 6  THEN 8
         WHEN 7  THEN 9
         WHEN 8  THEN 10
         WHEN 9  THEN 11
         WHEN 10 THEN 12
         WHEN 11 THEN 1
         WHEN 12 THEN 2
     END)
     THEN Quantity (SND) * (copy)
     ELSE NULL
END as carry_out_(dynamic)_(copy_2),
        IF Category (copy) = 'Govt Carry Out' 
AND MONTH(Period) = 
    CASE Parameters.Parameter 2 3
        WHEN 1  THEN 12
        WHEN 2  THEN 1
        WHEN 3  THEN 2
        WHEN 4  THEN 3
        WHEN 5  THEN 4
        WHEN 6  THEN 5
        WHEN 7  THEN 6
        WHEN 8  THEN 7
        WHEN 9  THEN 8
        WHEN 10 THEN 9
        WHEN 11 THEN 10
        WHEN 12 THEN 11
    END
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as carry_out_(dynamic)_(copy),
        IF Category (copy) = 'Carry Out' 
AND    (CropMonthSeq = 'M03'
     OR CropMonthSeq = 'M06'
     OR CropMonthSeq = 'M09'
     OR CropMonthSeq = 'M12')
    THEN QtyDefaultUnit
    ELSE NULL
END as carry_out_per_quarter_(default)_(copy),
        IFNULL(Category,'Shipment Export') as category_(copy),
         as category_(group),
        IFNULL(CategoryGroup,'Shipment Data') as category_group_(copy),
         as category_group_(group),
         as commodity_(brz_capacity),
         as commodityorigin_(level_3,_corn)_(copy),
         as commodityorigin_(level_3,_eu)_(copy),
         as commodityorigin_(level_3,_eu),
         as commodityorigin_(level_3,_oils)_(copy),
         as commodityorigin_(level_3,_soy_beans)_(copy_2),
         as commodityorigin_(level_3,_soy_beans)_(copy),
         as commodityorigin_(level_3,_soy_meal)_(copy),
         as commodityorigin_(level_3,_wheat)_(copy_2)_1008524876527824896,
         as commodityorigin_(level_3,_wheat)_(copy),
         as commodityorigin_(level_3,_corn)_(copy),
         as commodityorigin_(level_4,_corn)_(copy_2),
         as commodityorigin_(level_4,_corn)_(copy),
        IF Category (copy) = 'Carry Out'
    THEN Quantity * (copy 2)
    ELSE NULL
END as demand_(copy)_1397241823318720517,
         as dest._(level_2)_*_(demand_matrix),
         as dest._group_(level_1,_corn)_(copy)_1000080632292859904,
         as dest._group_(level_1,_corn)_v2,
         as dest._group_(level_1,_oils_-_po_-_eu)_(copy)_239535223290892290,
         as dest._group_(level_1,_oils_-_po)*,
         as dest._group_(level_1,_oils_-_rso_-_eu)_(copy)_239535223290916867,
         as dest._group_(level_1,_oils_-_rso)*_,
         as dest._group_(level_1,_oils_-_sbo_-_eu)_(copy)_239535223290937348,
         as dest._group_(level_1,_oils_-_sbo)*,
         as dest._group_(level_1,_oils_-_sfo_-_eu)_(copy)_239535223290957829,
         as dest._group_(level_1,_oils_-_sfo)*,
         as dest._group_(level_1,_soy_beans_eu)_(group),
         as dest._group_(level_1,_soy_beans/meal)_(copy)_794322424336744448,
         as dest._group_(level_1,_wheat)_v2,
         as dest._group_(level_2,_corn)_(copy)_1000080632292966401,
         as dest._group_(level_2,_corn)_v2,
         as dest._group_(level_2,_oils_-_po_-_eu)_(copy)_924082365990629380,
         as dest._group_(level_2,_oils_-_po)*_(copy)_547187366505148416,
         as dest._group_(level_2,_oils_-_po)*_(copy)_547187366510571521,
         as dest._group_(level_2,_oils_-_po)*_(copy)_547187366513963011,
         as dest._group_(level_2,_oils_-_rso_-_eu)_(copy)_924082365990588418,
         as dest._group_(level_2,_oils_-_sbo)_(copy)_924082365985001472,
         as dest._group_(level_2,_oils_-_sfo_-_eu)_(copy)_924082365990612995,
         as dest._group_(level_2,_soy_beans_eu),
         as dest._group_(level_2,_soy_beans/meal)_(copy)_794322424336855041,
         as dest._group_(level_2,_wheat)_v2,
         as dest._group_(level_3,_eu)_(copy)_880453736821772288,
         as destination_(intraregional_groups),
        IF Category (copy) = 'Domestic Ethanol' THEN Quantity Dynamic with Aggregations * (copy)
ELSEIF Category (copy) = 'Domestic Feed / Res' THEN Quantity Dynamic with Aggregations * (copy)
ELSEIF Category (copy) = 'Domestic Food' THEN Quantity Dynamic with Aggregations * (copy)
ELSEIF Category (copy) = 'Domestic FSI' THEN Quantity Dynamic with Aggregations * (copy)
ELSEIF Category (copy) = 'Domestic Seed' THEN Quantity Dynamic with Aggregations * (copy)
    ELSE NULL
END as domestic_consumption_(default)_(copy),
        IF Category (copy) = 'Domestic Ethanol' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Feed / Res' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Food' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic FSI' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Seed' THEN Quantity * (copy 2)
    ELSE NULL
END as domestic_consumption_(dynamic)_(copy),
        IF Category (copy) = 'Domestic Ethanol' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Feed / Res' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Food' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic FSI' THEN Quantity * (copy 2)
ELSEIF Category (copy) = 'Domestic Seed' THEN Quantity * (copy 2)
    ELSE NULL
END as domestic_use_(default)_(copy),
         as dynamic_period_(mmm)_(group)_(copy),
         as dynamic_period_(mmm)_(group),
        INT(MID(Calculation_1146447644122546176,2,1)) as dynamic_period_(qq)_*_(copy),
         as dynamic_period_(qq)_*_(group),
         as dynamic_period_(qq-mmm)_*_(group),
        IF INT(Period (MM) (copy)) < Parameters.Parameter 2
    THEN RIGHT("0" + STR(IF INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 = -1 
                            THEN 99 
                            ELSE INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 
                         END),2)
         + "/" + RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
ELSEIF  INT(Period (MM) (copy))>= Parameters.Parameter 2
    THEN RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
         + "/" + RIGHT("0"+ STR(INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))+1),2)
END as dynamic_period_(yy/yy)_*_(copy_2)_1555149311021072384_1,
        IF INT(Period (MM) (copy)) < Parameters.Parameter 2
    THEN RIGHT("0" + STR(IF INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 = -1 
                            THEN 99 
                            ELSE INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 
                         END),2)
         + "/" + RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
ELSEIF  INT(Period (MM) (copy))>= Parameters.Parameter 2
    THEN RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
         + "/" + RIGHT("0"+ STR(INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))+1),2)
END as dynamic_period_(yy/yy)_*_(copy_2)_1555149311021072384,
        // Added a one-to-one copy so that users can use dynamic fields with 
// dynamic periods to simplefy the usage of this Tableau data source.

Calculation_804455546385244163 as dynamic_period_(yy/yy)_*_(copy),
        // Added a one-to-one copy so that users can use dynamic fields with 
// dynamic periods to simplefy the usage of this Tableau data source.

Period (YYYY-MM) (copy) as dynamic_period_(yyyy)_*_(copy),
        IF Category (copy) = 'Exports'
    THEN Quantity * (copy 2)
    ELSE NULL
END as exportable_surplus_(default)_(copy),
        IF Category (copy) = 'Production'
    THEN Quantity * (copy 2)
    ELSE NULL
END as exportable_surplus_(dynamic)_(copy)_(copy)_1600748244288135169,
        IF Category (copy) = 'Exports'
    THEN Quantity * (copy 2)
    ELSE NULL
END as exportable_surplus_(dynamic)_(copy)_1397241823314243586,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='22/23'
THEN Exportable Surplus (Default) (copy)
ELSE NULL
END)} as exportable_surplus_21/22_(copy)_1753026166083575808,
        IF Category (copy) = 'Govt Carry In' 
AND CropMonthSeq = 'M01'
    THEN QtyDefaultUnit
    ELSE NULL
END as govt_carry_in_(default)_(copy),
        IF Category (copy) = 'Govt Carry In' 
AND (MONTH(Period) = Parameters.Parameter 2 3
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 4
         WHEN 2  THEN 5
         WHEN 3  THEN 6
         WHEN 4  THEN 7
         WHEN 5  THEN 8
         WHEN 6  THEN 9
         WHEN 7  THEN 10
         WHEN 8  THEN 11
         WHEN 9  THEN 12
         WHEN 10 THEN 1
         WHEN 11 THEN 2
         WHEN 12 THEN 3
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 7
         WHEN 2  THEN 8
         WHEN 3  THEN 9
         WHEN 4  THEN 10
         WHEN 5  THEN 11
         WHEN 6  THEN 12
         WHEN 7  THEN 1
         WHEN 8  THEN 2
         WHEN 9  THEN 3
         WHEN 10 THEN 4
         WHEN 11 THEN 5
         WHEN 12 THEN 6
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 10
         WHEN 2  THEN 11
         WHEN 3  THEN 12
         WHEN 4  THEN 1
         WHEN 5  THEN 2
         WHEN 6  THEN 3
         WHEN 7  THEN 4
         WHEN 8  THEN 5
         WHEN 9  THEN 6
         WHEN 10 THEN 7
         WHEN 11 THEN 8
         WHEN 12 THEN 9
     END)
     THEN Quantity (SND) * (copy)
     ELSE NULL
END as govt_carry_in_(dynamic)_(copy),
        IF Category (copy) = 'Govt Carry In' 
AND    (CropMonthSeq = 'M01'
     OR CropMonthSeq = 'M04'
     OR CropMonthSeq = 'M07'
     OR CropMonthSeq = 'M10')
    THEN QtyDefaultUnit
    ELSE NULL
END as govt_carry_in_per_quarter_(default)_(copy),
        IF Category (copy) = 'Govt Carry Out' 
AND CropMonthSeq = 'M12'
    THEN QtyDefaultUnit
    ELSE NULL
END as govt_carry_out_(default)_(copy),
        IF Category (copy) = 'Govt Carry Out' 
AND (MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 12
         WHEN 2  THEN 1
         WHEN 3  THEN 2
         WHEN 4  THEN 3
         WHEN 5  THEN 4
         WHEN 6  THEN 5
         WHEN 7  THEN 6
         WHEN 8  THEN 7
         WHEN 9  THEN 8
         WHEN 10 THEN 9
         WHEN 11 THEN 10
         WHEN 12 THEN 11
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 9
         WHEN 2  THEN 10
         WHEN 3  THEN 11
         WHEN 4  THEN 12
         WHEN 5  THEN 1
         WHEN 6  THEN 2
         WHEN 7  THEN 3
         WHEN 8  THEN 4
         WHEN 9  THEN 5
         WHEN 10 THEN 6
         WHEN 11 THEN 7
         WHEN 12 THEN 8
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 6
         WHEN 2  THEN 7
         WHEN 3  THEN 8
         WHEN 4  THEN 9
         WHEN 5  THEN 10
         WHEN 6  THEN 11
         WHEN 7  THEN 12
         WHEN 8  THEN 1
         WHEN 9  THEN 2
         WHEN 10 THEN 3
         WHEN 11 THEN 4
         WHEN 12 THEN 5
     END
     OR 
     MONTH(Period) = 
     CASE Parameters.Parameter 2 3
         WHEN 1  THEN 3
         WHEN 2  THEN 4
         WHEN 3  THEN 5
         WHEN 4  THEN 6
         WHEN 5  THEN 7
         WHEN 6  THEN 8
         WHEN 7  THEN 9
         WHEN 8  THEN 10
         WHEN 9  THEN 11
         WHEN 10 THEN 12
         WHEN 11 THEN 1
         WHEN 12 THEN 2
     END)
     THEN Quantity (SND) * (copy)
     ELSE NULL
END as govt_carry_out_(dynamic)_(copy),
        IF Category (copy) = 'Govt Carry Out' 
AND    (CropMonthSeq = 'M03'
     OR CropMonthSeq = 'M06'
     OR CropMonthSeq = 'M09'
     OR CropMonthSeq = 'M12')
    THEN QtyDefaultUnit
    ELSE NULL
END as govt_carry_out_per_quarter_(default)_(copy_2),
        IF Category (copy) = 'Carry Out' 
AND    (CropMonthSeq = 'M03'
     OR CropMonthSeq = 'M06'
     OR CropMonthSeq = 'M09'
     OR CropMonthSeq = 'M12')
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as govt_carry_out_per_quarter_(default)_(copy),
        IF Parameters.Forecast to Actual (YYYY-MM) (copy 2) = 'Inclusive' 
    THEN Calculation_1038079781623758848 
    ELSE Calculation_795729826980593664 
END as line-up_(shipment)_*_(copy),
        IF Parameters.Forecast to Actual (YYYY-MM) (copy 2) = 'Inclusive' 
    THEN Quantity (Custom) * (copy) 
    ELSE Quantity (ExIR, Custom) * (copy) 
END as line-up_*_(copy)_1816920976333086721,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='21/22'
THEN Exportable Surplus (Default) (copy)
ELSE NULL
END)} as line-ups_21/22_(copy)_1274518700893650946,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='22/23'
THEN Line-up * (copy)_1816920976333086721
ELSE NULL
END)} as line-ups_21/22_(copy)_1753026166083878913,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='22/23'
THEN Actual * (copy)
ELSE NULL
END)} as line-ups_22/23_(copy)_756041825535918082,
         as origin_(eu_together),
        CommodityOrigin (level 3, EU) as origin_(level_3)_(copy),
        IF Parameters.Commodity (copy) = "Soyoil" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN CommodityOrigin (level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeseed" THEN CommodityOrigin (level 3, Wheat) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunseeds" THEN CommodityOrigin (level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN CommodityOrigin (level 3, Soy Beans) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapemeal" THEN CommodityOrigin (level 3, Soy Beans) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunmeal" THEN CommodityOrigin (level 3, Soy Beans) (copy)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN CommodityOrigin (level 3, corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Corn" THEN CommodityOrigin (level 3, EU) (copy)
ELSEIF Parameters.Commodity (copy) = "Barley" THEN CommodityOrigin (level 3, Wheat) (copy 2)_1008524876527824896
END as origin_(level_3)_*_(copy_2),
        IF Parameters.Commodity (copy) = "Soyoil" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN Origin Group (Level 3, Oils) (copy)
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN Origin Group (Level 3, Soy Beans) (copy)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN Origin Group (Level 3, Soy Meal) (copy)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN Origin Group (Level 3,Wheat)  (copy)
ELSEIF Parameters.Commodity (copy) = "Corn" THEN Origin Group (Level 3, Corn) (copy)
END as origin_(level_3)_*_(copy),
         as origin_(level_3)_*_(group),
        CommodityOrigin as origin_(level_4)_(copy),
        IF Parameters.Commodity (copy) = "Soyoil" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Soyoil EU" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapeoil EU" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunoil EU" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Palmoil EU" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Soybeans" THEN CommodityOrigin (level 3, Soy Beans) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Rapeseed" THEN CommodityOrigin (level 4, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunseeds" THEN CommodityOrigin (level 3, Soy Beans) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Soymeal" THEN CommodityOrigin (level 3, Soy Meal) (copy)
ELSEIF Parameters.Commodity (copy) = "Rapemeal" THEN CommodityOrigin (level 3, Soy Meal) (copy)
ELSEIF Parameters.Commodity (copy) = "Sunmeal" THEN CommodityOrigin (level 3, Soy Meal) (copy)
ELSEIF Parameters.Commodity (copy) = "Wheat" THEN CommodityOrigin (level 4, Corn) (copy 2)
ELSEIF Parameters.Commodity (copy) = "Corn" THEN CommodityOrigin (level 3, Corn) (copy)
ELSEIF Parameters.Commodity (copy) = "Barley" THEN CommodityOrigin (level 4, Corn) (copy 2)
END as origin_(level_4)_*_(copy),
         as origin_group_(level_3,_corn)_(copy),
         as origin_group_(level_3,_eu)_(copy_2)_1405686046712967168,
         as origin_group_(level_3,_eu)_(copy),
         as origin_group_(level_3,_oils)_(copy_2),
         as origin_group_(level_3,_oils)_(copy),
         as origin_group_(level_3,_soy_beans)_(copy_2),
         as origin_group_(level_3,_soy_beans)_(copy),
         as origin_group_(level_3,_soy_meal)_(copy_2),
         as origin_group_(level_3,_soy_meal)_(copy),
         as origin_group_(level_3,wheat)__(copy_2),
         as origin_group_(level_3,wheat)__(copy),
        // Added a one-to-one copy so that users can use dynamic fields with 
// dynamic periods to simplefy the usage of this Tableau data source.

Calculation_804455546385559556 as period_(mm)_(copy),
        // Hardcoded to avoid differeces with regional settings.

CASE MONTH(Period)
    WHEN 1  THEN 'Jan'
    WHEN 2  THEN 'Feb'
    WHEN 3  THEN 'Mar'
    WHEN 4  THEN 'Apr'
    WHEN 5  THEN 'May'
    WHEN 6  THEN 'Jun'
    WHEN 7  THEN 'Jul'
    WHEN 8  THEN 'Aug'
    WHEN 9  THEN 'Sep'
    WHEN 10 THEN 'Oct'
    WHEN 11 THEN 'Nov'
    WHEN 12 THEN 'Dec'
END as period_(mmm)_(copy_2),
        // Added a one-to-one copy so that users can use dynamic fields with 
// dynamic periods to simplefy the usage of this Tableau data source.

Period (Mmm) (copy 2) as period_(mmm)_(copy),
        IF Parameters.Parameter 3 1 = "Quarter" 
    THEN Calculation_1146447644138991618
ELSEIF Parameters.Parameter 3 1 = "Month" 
    THEN Period (YYYY-MM) (copy)
END as period_(month_or_quarter)_(copy),
        INT(MID(Calculation_130041443275886594,2,1)) as period_(qq)_(copy),
        STR(Calculation_804455546385244163) + 
IF Calculation_804455546385559556 < 10 
  THEN '-0' + STR(Calculation_804455546385559556)
  ELSE '-' + STR(Calculation_804455546385559556) 
END as period_(yyyy-mm)_(copy),
        sum(Act 21/22 (copy)_338332926771908608)/sum(Proj 21/22 (copy)_1949495690455158784) as progress_(copy)_1151514138104168448,
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='20/21'
THEN Projection * (copy)
ELSE NULL
END)} as proj_(copy),
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='21/22'
THEN Projection * Corn (copy)_1108167023644725249
ELSE NULL
END)} as proj_20/21_(copy)_1372472030682599424,
        SUM(Line-ups 22/23 (copy)_756041825535918082)/SUM(Proj 21/22 (copy)_1949495690455158784) as proj_21/22_(copy)_1181631998128513024,
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='22/23'
THEN Projection * Corn (copy)_1108167023644725249
ELSE NULL
END)} as proj_21/22_(copy)_1949495690455158784,
        IF Calculation_215046905796349953='Canada' and Dynamic Period (YYYY) * (copy) > Parameters.Forecast to Actual (YYYY-MM) (EU) (copy)_837388065217126400 THEN Calculation_625155928475000833
ELSEIF Calculation_215046905796349953<>'Canada' and Dynamic Period (YYYY) * (copy) > Parameters.Parameter 8 
    THEN Calculation_625155928475000833
    ELSE Actual * (copy)
END as projection_*_(copy),
        IF Period (YYYY-MM) (copy) > Parameters.Parameter 8 1 
    THEN Calculation_625155928475000833
    ELSE Quantity (Projection, Default) * (copy)
END as projection_*_(copy)_340866237474562048,
        Projection * Corn (copy)_1108167023644725249 as projection_*_corn_(copy)_1397241823314350083,
        IF SUM(Projection * (copy)) = WINDOW_MAX(SUM(Projection * (copy))) THEN "Max"
ELSEIF SUM(Projection * (copy)) = WINDOW_MIN(SUM(Projection * (copy))) THEN "Min"
ELSE "Neutral"
END as projection_label_(copy),
        IF SUM(Actual * (copy)) = WINDOW_MAX(SUM(Actual * (copy))) THEN "Max"
ELSEIF SUM(Actual * (copy)) = WINDOW_MIN(SUM(Actual * (copy))) THEN "Min"
ELSE "Neutral"
END as projection_min/max_(copy),
        IF DataType = 'GLENCORE-LINEUP' 
    THEN Calculation_843017618144940032 
    ELSE NULL 
END as quantity_(custom)_*_(copy),
        IF (Origin (EU together) = Destination (IntraRegional Groups))
    THEN NULL
    ELSE Quantity (Custom) * (copy) 
END as quantity_(exir,_custom)_*_(copy),
        IF (Origin (EU together) = Destination (IntraRegional Groups))
    THEN NULL
    ELSE Quantity (Line-up) * (copy) 
END as quantity_(exir,_line-up)_*_(copy),
        IF DataType = 'GLENCORE-FORECAST' 
    THEN Calculation_843017618144940032 
    ELSE NULL 
END as quantity_(line-up)_*_(copy),
        IF Period (YYYY-MM) (copy) > Parameters.Forecast to Actual (YYYY-MM) (copy)
    THEN Actual (Shipment) * (copy)
    ELSE Line-up (Shipment) * (copy)
END as quantity_(projection,_default)_*_(copy),
        Calculation_843017618144940032 as quantity_(snd)_*_(copy),
        CASE Category (copy)
    WHEN 'Carry In'       THEN Carry In (Default) (copy)
    WHEN 'Carry Out'      THEN Carry Out (Default) (copy)
    WHEN 'Govt Carry In'  THEN Carry In (Dynamic) (copy)
    WHEN 'Govt Carry Out' THEN Carry Out (Dynamic) (copy)
    ELSE Quantity (SND) * (copy)
END as quantity_*_(copy_2),
        IF Category (copy) = 'Carry In' 
AND CropMonthSeq = 'M01'
    THEN Quantity (SND) * (copy)
    ELSE NULL
END as quantity_*_(copy),
        IF Origin (EU together)='South Africa' and Category (copy)='Yield' and Grade='White' THEN NULL
ELSEIF Origin (EU together)='South Africa' and Category (copy)='Yield'  THEN
{ FIXED Category Group (group),Category (copy),CropYear,Period (Mmm) (copy 2),Origin (EU together),Commodity,Grade,AsOfDate:SUM(Quantity Dynamic with Aggregations * (copy))*0.9}
ELSE Quantity Dynamic with Aggregations * (copy)
END as quantity_default_*_(copy_2)_1188105935875846144,
        CASE Category (copy)
    WHEN 'Carry In'       THEN Carry In (Default) (copy 3)
    WHEN 'Carry Out'      THEN Govt Carry Out per Quarter (Default) (copy)
    WHEN 'Govt Carry In'  THEN Carry In per Quarter (Default) (copy)
    WHEN 'Govt Carry Out' THEN Carry Out (Default) (copy 3)
    ELSE Quantity (SND) * (copy)
END as quantity_default_*_(copy),
        CASE Category (copy)
    WHEN 'Carry In'       THEN Carry In per Quarter (Default) (copy 2)
    WHEN 'Carry Out'      THEN Carry Out per Quarter (Default) (copy)
    WHEN 'Govt Carry In'  THEN Govt Carry In per Quarter (Default) (copy)
    WHEN 'Govt Carry Out' THEN Govt Carry Out per Quarter (Default) (copy 2)
    ELSE QtyDefaultUnit
END as quantity_default_per_quarter_*_(copy),
        CASE Category (copy)
    WHEN 'Carry In'       THEN Carry In (Dynamic) (copy 2)
    WHEN 'Carry Out'      THEN Carry Out (Dynamic) (copy 2)
    WHEN 'Govt Carry In'  THEN Govt Carry In (Dynamic) (copy)
    WHEN 'Govt Carry Out' THEN Govt Carry Out (Dynamic) (copy)
    ELSE Quantity (SND) * (copy)
END as quantity_dynamic_*_(copy),
        CASE Category (copy)
    WHEN 'Carry In'       THEN Quantity * (copy)
    WHEN 'Carry Out'      THEN Carry In (copy)
    WHEN 'Govt Carry In'  THEN Carry In (Default) (copy 2)
    WHEN 'Govt Carry Out' THEN Carry Out (Default) (copy 2)
    ELSE Quantity (SND) * (copy)
END as quantity_dynamic_with_aggregations_*_(copy),
        SUM(Carry Out (Default) (copy)) / SUM(Total Demand (Default) (copy)) as stock_to_use_(default)_(copy),
        Carry Out (Default) (copy) / Total Demand (Default) (copy) as stock_to_use_(dynamic)_(copy),
        IFNULL(SubCategory,'') as sub_category_(copy),
        IF Category (copy) = 'Domestic Ethanol'
or Category (copy) = 'Domestic Feed / Res'
or Category (copy) = 'Domestic FSI'
    THEN Quantity * (copy 2)
    ELSE NULL
END as supply_(copy)_1600748244294778883,
        SUM(Exportable Surplus (Default) (copy)) - SUM(Calculation_608830385425960960) as surplus_/_deficit_(default)_(copy),
        SUM(Exportable Surplus (Default) (copy)) - SUM(Projection * (copy)) as surplus_/_deficit_(dynamic)_(copy),
        SUM(Exportable Surplus (Default) (copy)) - SUM(Projection * Corn (copy)_1108167023644725249) as surplus_/_deficit_(dynamic)_corn_(copy)_418271855744917504,
        IF Category Group (copy) = 'Demand'
    THEN Quantity * (copy 2)
    ELSE NULL
END as total_demand_(default)_(copy),
         as ytd_shipment_(group_sep-aug))_(copy),
        "R" as parameter_1_1_1,
        9 as parameter_2_3,
        "Month" as parameter_3_1,
        "2025-03" as parameter_8_1,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='19/20'
THEN Projection * (copy) 1
ELSE NULL
END)} as 18/19_(copy)_1,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='20/21'
THEN Projection * (copy) 1
ELSE NULL
END)} as 19/20_(copy)_1372472030682869761_1,
        { FIXED Calculation_804455546384973826,AsOfDate,DataSource,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='21/22'
THEN Projection * (copy) 1
ELSE NULL
END)} as 20/21_(copy)_1949495690456383489_1,
        IF ISNULL(SUM(Projection * (copy))) = FALSE THEN
    RUNNING_SUM(SUM(Projection * (copy)))
END as calculation_1017250622268907521,
        sum(Calculation_1099441275466203139)/sum(Proj (copy) 1) as calculation_1099441275457744897_1,
        sum(Calculation_1099441275466203139)/sum(Proj (copy)) as calculation_1099441275457744897,
        IF Calculation_804455546384973826='19/20'
THEN Projection * (copy) 1
ELSE NULL
END as calculation_1099441275466469380_1,
        SUM(Proj 20/21 (copy)_1372472030682599424 1)-SUM(sqlproxy.1184uzq0lnhd681emq2qs0yk33y6.Calculation_930274800807022599) as calculation_1392175277234503683_1,
        SUM(Act 21/22 (copy)_338332926771908608)-SUM(Proj 21/22 (copy)_1949495690455158784 1) as calculation_1816920976331595776_1,
        sum(Act 21/22 (copy)_338332926771908608)/sum(Proj 21/22 (copy)_1949495690455158784 1) as progress_(copy)_1151514138104168448_1,
        sum(Act (copy)_1053842374137585667)/sum(Proj 20/21 (copy)_1372472030682599424 1) as progression_(copy)_1653384030878343168_1,
        sum(Act (copy)_1053842374137585667)/sum(Proj 20/21 (copy)_1372472030682599424) as progression_(copy)_1653384030878343168,
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Calculation_130041442962415617, Origin,Calculation_1146447644456341507: sum (
IF  Calculation_804455546384973826='20/21'
THEN Projection * (copy) 1
ELSE NULL
END)} as proj_(copy)_1,
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='21/22'
THEN Projection * Corn (copy)_1108167023644725249 1
ELSE NULL
END)} as proj_20/21_(copy)_1372472030682599424_1,
        SUM(Line-ups 22/23 (copy)_756041825535918082)/SUM(Proj 21/22 (copy)_1949495690455158784 1) as proj_21/22_(copy)_1181631998128513024_1,
        { FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='22/23'
THEN Projection * Corn (copy)_1108167023644725249 1
ELSE NULL
END)} as proj_21/22_(copy)_1949495690455158784_1,
        IF Calculation_215046905796349953='Canada' and Dynamic Period (YYYY) * (copy) > Parameters.Forecast to Actual (YYYY-MM) (EU) (copy)_837388065217126400 THEN Calculation_625155928475000833
ELSEIF Calculation_215046905796349953<>'Canada' and Dynamic Period (YYYY) * (copy) > Parameters.Parameter 8 
    THEN Calculation_625155928475000833
    ELSE Actual * (copy)
END as projection_*_(copy)_1,
        IF Calculation_215046905796349953='European Union' and Dynamic Period (YYYY) * (copy) > Parameters.Forecast to Actual (YYYY-MM) (copy 3)_790944748003606529 THEN Calculation_625155928475000833
ELSEIF Calculation_215046905796349953='European Union' and Dynamic Period (YYYY) * (copy) <= Parameters.Forecast to Actual (YYYY-MM) (copy 3)_790944748003606529 THEN Actual * (copy)
ELSEIF Calculation_215046905796349953<>'European Union' THEN Projection * (copy) 1

END as projection_*_corn_(copy)_1108167023644725249_1,
        Projection * Corn (copy)_1108167023644725249 1 as projection_*_corn_(copy)_1397241823314350083_1,
        IF SUM(Projection * (copy) 1) = WINDOW_MAX(SUM(Projection * (copy) 1)) THEN "Max"
ELSEIF SUM(Projection * (copy) 1) = WINDOW_MIN(SUM(Projection * (copy) 1)) THEN "Min"
ELSE "Neutral"
END as projection_label_(copy)_1,
        IF ISNULL(SUM(Actual * (copy))) = FALSE THEN
    RUNNING_SUM(SUM(Actual * (copy)))
END as running_total_(copy)_844706471130980357,
        SUM(Exportable Surplus (Default) (copy)) - SUM(Projection * (copy) 1) as surplus_/_deficit_(dynamic)_(copy)_1,
        SUM(Exportable Surplus (Default) (copy)) - SUM(Projection * Corn (copy)_1108167023644725249 1) as surplus_/_deficit_(dynamic)_corn_(copy)_418271855744917504_1
    from source_data
)

select * from calculated_metrics