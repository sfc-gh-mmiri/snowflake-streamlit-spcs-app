-- Query 1: Creating table QLD_HISTORICAL_FIRES
-- This query should take around 8 minutes to be created (using an X-Large warehouse)
create or replace table qld_historical_fires as 
with cte_intersecting_properties as 
(
    select 
        fires.LABEL fire_label,
        count(prop.lotplan) count_of_intersecting_properties
    from 
    wildfire__fire_locations__fire_stations__australia__free.fire_locations_aus_free.qld_historical_fire_records_gda2020 fires
    left join cadastre__boundaries__attributes__australia.cadastre_aus_free.qld_cadastre_lot_gda2020 prop
    on st_intersects(fires.geometry,prop.geometry)
    where fires.IGNITIONDA>='2000-01-01'::date and fires.geometry is not null
    group by fires.LABEL
)
select
    f.LABEL fire_label, 
    case f.TYPE 
    when 'PB' then 'Planned Burn'
    when 'WF' then 'Wildfire'
    else 'Unknown' end fire_type, 
    initcap(f.BURNSTATUS) burn_status, 
    f.GENERAL_LO general_location, 
    f.OWNING_AGE owning_agency, 
    f.IGNITIONDA ignition_date, 
    f.OUTDATE out_date, 
    f.OUTYEAR out_year, 
    f.MONTH out_month,  
    f.PCT_BURN percentage_burnt, 
    f.AREA_HA area_hectare, 
    f.GEOMETRY,
    to_geography(st_asgeojson(f.geometry)) geography,    
    ifnull(prop.count_of_intersecting_properties,0) count_of_intersecting_properties
from 
wildfire__fire_locations__fire_stations__australia__free.fire_locations_aus_free.qld_historical_fire_records_gda2020 f
left join cte_intersecting_properties prop
on f.LABEL = prop.fire_label
where f.IGNITIONDA>='2000-01-01'::date and f.geometry is not null;



-- Query 2: Creating table QLD_FIRE_BRIGADE_STATIONS
create table qld_fire_brigade_stations as 
with cte_brigades as 
(
    select
        brigade_id,
        brigade,
        initcap(rural_area) rural_area,
        class_id,        
        geometry,
        to_geography(st_asgeojson(geometry)) geography
    from
        WILDFIRE__FIRE_LOCATIONS__FIRE_STATIONS__AUSTRALIA__FREE.FIRE_LOCATIONS_AUS_FREE.QLD_RURAL_FIRE_BRIGADE_BOUNDARIES_GDA2020
    where geometry is not null
),
cte_stations as 
(
    select
        station,
        address,
        locality,
        altaddress alternative_address,
        crewing,
        levytype,
        long_gda2020 longitude,
        lat_gda2020 latitude,
        geometry,
        geography
    from wildfire__fire_locations__fire_stations__australia__free.fire_locations_aus_free.qld_urban_fire_station_locations_gda2020
    union all
    select
        station,
        address,
        locality,
        altaddress alternative_address,
        null crewing,
        null levytype,
        long_gda2020 longitude,
        lat_gda2020 latitude,
        geometry,
        geography 
    from wildfire__fire_locations__fire_stations__australia__free.fire_locations_aus_free.qld_rural_fire_station_locations_gda2020
)
select 
s.*,
b.brigade_id,
b.brigade brigade_name,
b.rural_area,
b.class_id brigade_class_id,
b.geography brigade_geography,
b.geometry brigade_geometry,
case when b.brigade_id is null then false else true end mapped_to_brigade 
from cte_stations s 
left join cte_brigades b
on st_contains(b.geography,s.geography);