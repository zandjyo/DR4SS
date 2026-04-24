SELECT
    to_char(racebase.haul.start_time, 'YYYY')                           AS year,
    racebase.haul.region,
    racebase.haul.stationid,
    racebase.haul.hauljoin,
    ( racebase.haul.start_latitude + racebase.haul.end_latitude ) / 2   AS mid_lat,
    ( racebase.haul.start_longitude + racebase.haul.end_longitude ) / 2 AS mid_lon   
FROM
    racebase.haul
WHERE
    racebase.haul.region
    -- insert region
    AND racebase.haul.abundance_haul = 'Y'
ORDER BY
    year,
    racebase.haul.region,
    racebase.haul.stationid