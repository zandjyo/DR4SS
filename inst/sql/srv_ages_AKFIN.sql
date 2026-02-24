SELECT
    'Survey' as "Source",
    racebase.specimen.weight / 1000 AS "Weight_kg",
    CASE
      When racebase.specimen.sex =1 then 'F'
      When racebase.specimen.sex =2 then 'M'
      When racebase.specimen.sex=3 then 'U'
      else 'U'
      END AS "Sex",
    racebase.specimen.age           AS "Age_yrs",
    racebase.specimen.length/10        AS "Length_cm",
    to_char(
        racebase.haul.start_time, 'mm'
    )                               AS "Month",
    to_char(
        racebase.haul.start_time, 'yyyy'
    )                               AS "Year"
FROM
     akr.species_translation
    INNER JOIN racebase.specimen ON akr.species_translation.from_code =racebase.specimen.species_code
    INNER JOIN racebase.specimen ON racebase.haul.cruisejoin = racebase.specimen.cruisejoin
                                    AND racebase.haul.hauljoin = racebase.specimen.hauljoin
WHERE
    akr.species_translation.to_code
    -- insert species
    AND racebase.specimen.region
    -- insert region
    AND racebase.haul.abundance_haul = 'Y'
    AND akr.species_translation.from_agency = 'RACE'
    AND akr.species_translation.to_agency = 'OBS' 