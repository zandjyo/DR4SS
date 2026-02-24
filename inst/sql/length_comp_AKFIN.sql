SELECT
    gap_products.akfin_sizecomp.year                  AS year,
    gap_products.akfin_sizecomp.sex                   AS sex, 
    gap_products.akfin_sizecomp.length_mm / 10        AS length,
    SUM(gap_products.akfin_sizecomp.population_count) AS total
FROM
    akr.species_translation
    INNER JOIN gap_products.akfin_sizecomp ON akr.species_translation.from_code = gap_products.akfin_sizecomp.species_code

WHERE
    gap_products.akfin_sizecomp.area_id
    -- insert area_id
    AND akr.species_translation.to_code
    -- insert species
    AND akr.species_translation.from_agency = 'RACE'
    AND akr.species_translation.to_agency = 'OBS' 
GROUP BY
    gap_products.akfin_sizecomp.year,
    gap_products.akfin_sizecomp.sex,
    gap_products.akfin_sizecomp.length_mm
ORDER BY
    year, 
    sex,
    length