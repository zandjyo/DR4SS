SELECT
    akr.species_translation.to_code                  AS species,
    gap_products.akfin_biomass.year                  AS year,
    SUM(gap_products.akfin_biomass.biomass_mt)       AS biomass,
    SUM(gap_products.akfin_biomass.population_count) AS population,
    SUM(gap_products.akfin_biomass.biomass_var)      AS varbio,
    SUM(gap_products.akfin_biomass.population_var)   AS varpop,
    SUM(gap_products.akfin_biomass.n_haul)           AS numhauls,
    SUM(gap_products.akfin_biomass.n_count)          AS numcaught
FROM
    akr.species_translation
    INNER JOIN gap_products.akfin_biomass ON akr.species_translation.from_code = gap_products.akfin_biomass.species_code
WHERE gap_products.akfin_biomass.area_id
    -- insert area_id
AND gap_products.akfin_biomass.survey_definition_id
     -- insert survey
AND akr.species_translation.to_code
    -- insert species
AND gap_products.akfin_biomass.year
    -- insert start_year
AND akr.species_translation.from_agency = 'RACE'
AND akr.species_translation.to_agency = 'OBS'
GROUP BY
    akr.species_translation.to_code,
    gap_products.akfin_biomass.year
ORDER BY
    year