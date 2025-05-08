with source as (
    select * from {{ source('sqlproxy.0s98eth1rpa3y91a3tudp05g1dij', 'raw_data') }}
),

renamed as (
    select
        -- Add column selection based on the fields in the datasource
        :measure_names, calculation_269934509093781504, calculation_269934509095219203, calculation_307370685664710656, charterer, combicargo, comment, commodities, commodity_origin_name_(wb)_(group), commodity_segment_name_(mapped), commodity_usage_name_(wb)_(copy), commodityorigin, createdby, createdon, crop_calendar_date_yyyy_(wb)_(copy), crop_calendar_date_yyyy-mm_(wb)_(copy), cropyear, cropyear_(copy), dischberth, dischberthterminalport, dischcountry, dischcountry_(group), dischport, dischterminal, discharge_area, dischargedate, grainsubtype, graintenderduedate, graintenderissuedate, graintenderissuer, grainuse, id, lastseenballastladen, lastseendwt, lastseendatetime, lastseendestination, lastseendraft, lastseeneta, lastseenspeed, lastseenvesselclass, lastseenzonelist, loadberth, loadberthterminalport, loadberthterminalport_(group), loadberthterminalport_(group)_1, loadcountry, loaddate, loadport, loadterminal, number_of_records, originalmessage, received, receiver, shipper, size, sourcename, sourcereportdate, status, statuschangedon, strategy, strategydischarea, strategydischsource, strategyloadarea, strategyloadsource, supplier, updatedby, updatedon, vessel, vesselimo
    from source
)

select * from renamed