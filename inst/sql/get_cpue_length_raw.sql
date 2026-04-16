SELECT
    to_char(racebase.haul.start_time, 'YYYY')                           AS year,
    racebase.haul.region,
    racebase.haul.stationid,
    racebase.haul.hauljoin,
    ( racebase.haul.start_latitude + racebase.haul.end_latitude ) / 2   AS mid_lat,
    ( racebase.haul.start_longitude + racebase.haul.end_longitude ) / 2 AS mid_lon,
    racebase.length.species_code,
    gap_products.akfin_cpue.cpue_nokm2,
    racebase.length.sex,
    racebase.length.length,
    racebase.length.frequency
FROM
         gap_products.akfin_cpue
    INNER JOIN racebase.haul ON gap_products.akfin_cpue.hauljoin = racebase.haul.hauljoin
    INNER JOIN racebase.length ON racebase.haul.cruisejoin = racebase.length.cruisejoin
                                  AND racebase.haul.hauljoin = racebase.length.hauljoin
                                  AND gap_products.akfin_cpue.species_code = racebase.length.species_code
WHERE
    racebase.length.species_code
    -- insert species
    AND racebase.length.region
    -- insert region
    AND racebase.haul.abundance_haul = 'Y'
ORDER BY
    year,
    racebase.haul.region,
    racebase.haul.stationid,
    racebase.length.sex,
    racebase.length.length