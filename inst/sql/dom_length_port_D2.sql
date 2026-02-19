SELECT
    norpac.debriefed_length.species,
    norpac.debriefed_length.nmfs_area           AS area,
    norpac.debriefed_length.year                AS year,
    TO_CHAR(norpac.debriefed_length.haul_offload_date, 'MM') AS month,
    norpac.debriefed_length.haul_offload_date   AS DELIVERY_DATE,
    norpac.debriefed_length.cruise,
    norpac.debriefed_length.permit,
    norpac.debriefed_offload_mv.delivery_vessel_adfg AS DELIVERING_VESSEL,
    CASE
        WHEN norpac.debriefed_length.gear IN (
            1,
            2,
            3,
            4
        ) THEN
            'TRW'
        WHEN norpac.debriefed_length.gear IN (
            6
        ) THEN
            'POT'
        WHEN norpac.debriefed_length.gear IN (
            5,
            7,
            9,
            10,
            11,
            68,
            8
        ) THEN
            'HAL'
    END AS gear,
    norpac.debriefed_length.haul_offload        AS haul,
    concat('P', TO_CHAR(norpac.debriefed_length.port_join)) AS haul_join,
    norpac.debriefed_length.landing_report_id,
    CASE
        WHEN norpac.debriefed_length.sex IN (
            'F'
        ) THEN
            '1'
        WHEN norpac.debriefed_length.sex IN (
            'M'
        ) THEN
            '2'
        WHEN norpac.debriefed_length.sex IN (
            'U'
        ) THEN
            '3'
    END AS sex,
    norpac.debriefed_length.length,
    norpac.debriefed_length.frequency           AS sum_frequency,
    '0' AS numb,
    'DOMESTIC PORT' AS source
FROM
    norpac.debriefed_length
    INNER JOIN norpac.debriefed_offload_mv ON norpac.debriefed_length.t_table = norpac.debriefed_offload_mv.t_table
                                              AND norpac.debriefed_length.cruise = norpac.debriefed_offload_mv.cruise
                                              AND norpac.debriefed_length.permit = norpac.debriefed_offload_mv.permit
                                              AND norpac.debriefed_length.haul_offload = norpac.debriefed_offload_mv.offload_number
WHERE
    norpac.debriefed_length.species
    -- insert species
    AND norpac.debriefed_length.nmfs_area
    -- insert region
    AND norpac.debriefed_length.year between 2008 and 2010
    AND norpac.debriefed_length.port_join IS NOT NULL