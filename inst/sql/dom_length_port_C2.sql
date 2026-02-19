SELECT
    NORPAC.debriefed_length.species,
    NORPAC.debriefed_length.nmfs_area AS area,
    NORPAC.debriefed_length.year AS year,
    TO_CHAR(NORPAC.debriefed_length.haul_offload_date, 'MM') AS month,
    NORPAC.debriefed_length.haul_offload_date AS HDAY,
    NORPAC.debriefed_length.cruise,
    NORPAC.debriefed_length.permit,
     CASE 
        WHEN NORPAC.debriefed_length.gear in (1,2,3,4) 
        THEN 'TRW'
        WHEN NORPAC.debriefed_length.gear in 6 
        THEN 'POT' 
        WHEN NORPAC.debriefed_length.gear in (5,7,9,10,11,68,8) 
        THEN 'HAL' 
     END                                AS GEAR,
    NORPAC.debriefed_length.haul_offload AS HAUL,
    concat('P', TO_CHAR(NORPAC.debriefed_length.port_join)) AS haul_join,
    NORPAC.debriefed_length.landing_report_id,
    CASE
        WHEN NORPAC.debriefed_length.sex IN ('F') 
        THEN  '1'
        WHEN NORPAC.debriefed_length.sex IN ('M')
        THEN  '2'
        WHEN NORPAC.debriefed_length.sex IN ('U')
        THEN '3'
    END AS sex,
    NORPAC.debriefed_length.length,
    NORPAC.debriefed_length.frequency AS SUM_FREQUENCY,
    council.comprehensive_ft.adfg_i_whole_pounds/2204.62 AS TONS_LANDED,
    '0' AS NUMB,
    'DOMESTIC PORT' AS SOURCE
FROM
         council.comprehensive_ft
    INNER JOIN norpac.debriefed_length ON council.comprehensive_ft.adfg_h_landing_report_number = norpac.debriefed_length.landing_report_id
    INNER JOIN akr.species_translation ON council.comprehensive_ft.adfg_i_species_code = akr.species_translation.from_code
                                          AND akr.species_translation.to_code = norpac.debriefed_length.species
WHERE
    akr.species_translation.to_code
    -- insert species
     AND NORPAC.debriefed_length.nmfs_area 
    -- insert region
    AND norpac.debriefed_length.year >= 2011
    AND norpac.debriefed_length.landing_report_id IS NOT NULL
    AND norpac.debriefed_length.port_join IS NOT NULL
    AND akr.species_translation.from_agency = 'ADFG'
    AND akr.species_translation.to_agency = 'OBS'
    