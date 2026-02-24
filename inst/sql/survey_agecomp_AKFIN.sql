SELECT
    gap_products.akfin_agecomp.year,
    gap_products.akfin_agecomp.age,
    SUM(gap_products.akfin_agecomp.population_count) AS "AGEPOP"
FROM
    akr.species_translation
    INNER JOIN gap_products.akfin_agecomp ON akr.species_translation.from_code = gap_products.akfin_agecomp.species_code
WHERE
    gap_products.akfin_agecomp.age >= 0
    AND  akr.species_translation.to_code
        -- insert species
    AND gap_products.akfin_agecomp.area_id 
        -- insert area_id
    AND gap_products.akfin_agecomp.year
        -- insert start_year 
    AND akr.species_translation.from_agency = 'RACE'
    AND akr.species_translation.to_agency = 'OBS'
GROUP BY
    gap_products.akfin_agecomp.year,
    gap_products.akfin_agecomp.age
ORDER BY
    gap_products.akfin_agecomp.year