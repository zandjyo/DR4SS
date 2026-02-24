SELECT
    gap_products.akfin_cruise.year                       AS year,
    COUNT(DISTINCT gap_products.akfin_length_v.hauljoin) AS hauls,
    SUM(gap_products.akfin_length_v.frequency)           AS lengths
FROM
    akr.species_translation
    INNER JOIN gap_products.akfin_length_v ON akr.species_translation.from_code = gap_products.akfin_length_v.species_code
    INNER JOIN gap_products.akfin_length_v ON gap_products.akfin_cruise.cruisejoin = gap_products.akfin_length_v.cruisejoin
WHERE
    akr.species_translation.to_code 
    -- insert species
    AND gap_products.akfin_cruise.survey_definition_id
    -- insert survey
    AND akr.species_translation.from_agency = 'RACE'
    AND akr.species_translation.to_agency = 'OBS'
GROUP BY
    gap_products.akfin_cruise.year,
    gap_products.akfin_length_v.species_code
ORDER BY
    year