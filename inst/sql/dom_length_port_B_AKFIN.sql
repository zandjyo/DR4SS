 SELECT
    norpac.domestic_port.fish_ticket_no,
    norpac.domestic_port.year,
    norpac.domestic_port.cruise,
    norpac.domestic_port.vessel,
    norpac.debriefed_length.vessel         AS permit,
    norpac.domestic_port.delivery_date,
    norpac.domestic_port.delivery,
    CASE 
        WHEN norpac.domestic_port.gear_type in (1,2,3,4) 
        THEN 'TRW'
        WHEN norpac.domestic_port.gear_type in 6 
        THEN 'POT' 
        WHEN norpac.domestic_port.gear_type in (5,7,9,10,11,68,8) 
        THEN 'HAL' 
     END                                AS GEAR, 
    norpac.domestic_port.nmfs_area_code AS AREA,
    norpac.domestic_port.delivering_vessel,
    concat('P', TO_CHAR(norpac.debriefed_length.port_join)) AS haul_join,
    norpac.debriefed_length.species,
    CASE
        WHEN norpac.debriefed_length.sex IN ('F') 
        THEN  '1'
        WHEN norpac.debriefed_length.sex IN ('M')
        THEN  '2'
        WHEN norpac.debriefed_length.sex IN ('U')
        THEN '3'
    END AS sex,
    norpac.debriefed_length.length,
    norpac.debriefed_length.frequency AS sum_frequency,
    'DOMESTIC PORT' AS SOURCE
FROM
    norpac.domestic_port
    INNER JOIN norpac.debriefed_length ON norpac.domestic_port.port_join = norpac.debriefed_length.port_join
WHERE
    norpac.domestic_port.fish_ticket_no IS NOT NULL
    AND norpac.domestic_port.year >= 1999
    AND norpac.domestic_port.year <= 2007
    AND norpac.domestic_port.nmfs_area_code
    -- insert region
    AND norpac.debriefed_length.species
    -- insert species
ORDER BY
    norpac.domestic_port.year,
    norpac.domestic_port.fish_ticket_no