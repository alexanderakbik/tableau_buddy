with source_data as (
    select * from {{ ref('stg_sqlproxy.1kpyl5p0e3iu3n15utyig0bois6v_(copy_2)') }}
),

calculated_metrics as (
    select
        *,
        -- Add calculated fields from Tableau
        "2023-03" as (ar)_line-up_to_custom_(yyyy-mm)_(copy)_(copy)_2,
        "2023-02" as (saf)_line-up_to_custom_(yyyy-mm)_(copy)_2,
        "2023-02" as (ser)_line-up_to_custom_(yyyy-mm)__(copy)_1,
        "2023-02" as (us)_line-up_to_custom_(yyyy-mm)_(copy_2)_2,
        "2023-02" as (us)_line-up_to_custom_(yyyy-mm)_(copy_3)_1740359830515621888_2,
        "2023-02" as (us)_line-up_to_custom_(yyyy-mm)_(copy)_2,
        "2023-02" as forecast_to_actual_(yyyy-mm)_(eu)_(copy)_837388065217126400_2,
        "2023-02" as forecast_to_actual_(yyyy-mm)_(copy_3)_790944748003606529_2,
        "2023-03" as line-up_to_custom_(yyyy-mm)_(copy)_2,
        SUM(Line-up (Shipment) * (copy))- sum(Actual (Shipment) * (copy)) as calculation_1213438689982754816,
        ZN(SUM(Actual * (copy))) - LOOKUP(ZN(SUM(Actual * (copy))), -1) as calculation_50384077325578244,
        IFNULL(SUM(Act 21/22 (copy)_338332926771908608),0)-IFNULL(SUM(Act (copy)_1053842374137585667),0) as calculation_50384077328175109,
        IIF(Calculation_50384077328175109=0, 'Exclude', 'Include') as calculation_50384077349441542,
        ZN({ FIXED Calculation_804455546384973826,AsOfDate,Calculation_1146447644138991618,Period (Mmm) (copy),Calculation_130041442962415617, Origin,Calculation_795729827015913473: sum (
IF  Calculation_804455546384973826='23/24'
THEN Projection * Corn (copy)_1108167023644725249
ELSE NULL
END)}) as proj_22/23_(copy)_398287099448393728,
        TOTAL(sum(Projection * (copy))) as calculation_1209216512209281024,
        IF     Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=1 THEN "Q1-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=1 THEN "Q2-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=1 THEN "Q3-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=1 THEN "Q4-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=1 THEN "Q4-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=2 THEN "Q1-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=2 THEN "Q2-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=2 THEN "Q3-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=2 THEN "Q4-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=2 THEN "Q4-NDJ"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=3 THEN "Q4-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=3 THEN "Q1-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=3 THEN "Q2-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=3 THEN "Q3-SON"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=3 THEN "Q4-DJF"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=4 THEN "Q4-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=4 THEN "Q1-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=4 THEN "Q2-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=4 THEN "Q3-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=4 THEN "Q3-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=5 THEN "Q4-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=5 THEN "Q1-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=5 THEN "Q2-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=5 THEN "Q3-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=5 THEN "Q3-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=6 THEN "Q3-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=6 THEN "Q4-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=6 THEN "Q1-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=6 THEN "Q2-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=6 THEN "Q2-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=7 THEN "Q3-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=7 THEN "Q4-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=7 THEN "Q1-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=7 THEN "Q2-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=7 THEN "Q2-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=8 THEN "Q3-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=8 THEN "Q4-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=8 THEN "Q1-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=8 THEN "Q2-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=8 THEN "Q2-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=9 THEN "Q2-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=9 THEN "Q3-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=9 THEN "Q4-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=9 THEN "Q1-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=9 THEN "Q1-SON"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=10 THEN "Q2-JFM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=10 THEN "Q3-AMJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=10 THEN "Q4-JAS"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=10 THEN "Q1-OND"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=10 THEN "Q1-OND"

ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=11 THEN "Q2-FMA"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=11 THEN "Q3-MJJ"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=11 THEN "Q4-ASO"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=11 THEN "Q1-NDJ"
ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=11 THEN "Q1-NDJ"

ELSEIF Period (Mmm) (copy 2)="Dec"  AND Parameters.Parameter 2=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Jan"  AND Parameters.Parameter 2=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Feb"  AND Parameters.Parameter 2=12 THEN "Q1-DJF"
ELSEIF Period (Mmm) (copy 2)="Mar"  AND Parameters.Parameter 2=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Apr"  AND Parameters.Parameter 2=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="May"  AND Parameters.Parameter 2=12 THEN "Q2-MAM"
ELSEIF Period (Mmm) (copy 2)="Jun"  AND Parameters.Parameter 2=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Jul"  AND Parameters.Parameter 2=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Aug"  AND Parameters.Parameter 2=12 THEN "Q3-JJA"
ELSEIF Period (Mmm) (copy 2)="Sep"  AND Parameters.Parameter 2=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Oct"  AND Parameters.Parameter 2=12 THEN "Q4-SON"
ELSEIF Period (Mmm) (copy 2)="Nov"  AND Parameters.Parameter 2=12 THEN "Q4-SON"
END as dynamic_period_(qq-mmm)_*_(copy)_2067996665394716672,
         as dynamic_period_(qq-mmm)_*_(group)_1,
        IF INT(Period (MM) (copy)) < Parameters.Parameter 2
    THEN RIGHT("0" + STR(IF INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 = -1 
                            THEN 99 
                            ELSE INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))-1 
                         END),2)
         + "/" + RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
ELSEIF  INT(Period (MM) (copy))>= Parameters.Parameter 2
    THEN RIGHT("0" + RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2),2)
         + "/" + RIGHT("0"+ STR(INT(RIGHT(LEFT(STR(Dynamic Period (YY/YY) * (copy)), 4),2))+1),2)
END as dynamic_period_(yy/yy)_*_(copy_3)_2175520109227528194
    from source_data
)

select * from calculated_metrics