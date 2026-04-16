SELECT
    to_char(racebase.cruise.start_date, 'YYYY')                           AS year,
    racebase.specimen.region,
    racebase.haul.stationid,
    racebase.haul.hauljoin,
    ( racebase.haul.end_latitude + racebase.haul.start_latitude ) / 2     AS mid_lat,
    ( racebase.haul.end_longitude + racebase.haul.start_longitude ) / 2 AS mid_lon,
    racebase.specimen.species_code,
    racebase.specimen.sex,
    racebase.specimen.length,
    racebase.specimen.age
FROM
         racebase.cruise
    INNER JOIN racebase.haul ON racebase.cruise.cruisejoin = racebase.haul.cruisejoin
    INNER JOIN racebase.specimen ON racebase.haul.cruisejoin = racebase.specimen.cruisejoin
                                    AND racebase.haul.hauljoin = racebase.specimen.hauljoin
WHERE
        racebase.specimen.species_code
        -- insert species
    AND racebase.specimen.region
        -- insert region
    AND racebase.specimen.age IS NOT NULL
    AND racebase.haul.abundance_haul = 'Y'
ORDER BY
    year