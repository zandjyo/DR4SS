SELECT
    norpac.debriefed_age.gear,
    norpac.debriefed_age.nmfs_area,
    norpac.debriefed_age.latdd_end,
    norpac.debriefed_age.londd_end,
    norpac.debriefed_age.species,
    norpac.debriefed_age.sex,
    norpac.debriefed_age.age,
    norpac.debriefed_age.length,
    norpac.debriefed_age.weight,
    norpac.debriefed_age.year,

    CASE
        WHEN norpac.debriefed_age.haul_join IS NOT NULL THEN
            CONCAT('H', TO_CHAR(norpac.debriefed_age.haul_join))
        WHEN norpac.debriefed_age.port_join IS NOT NULL THEN
            CONCAT('P', TO_CHAR(norpac.debriefed_age.port_join))
        ELSE NULL
    END AS haul_join,

    TO_CHAR(norpac.debriefed_haul.haul_date, 'mm') AS month

FROM norpac.debriefed_age

INNER JOIN norpac.debriefed_haul
    ON COALESCE(
           norpac.debriefed_age.haul_join,
           norpac.debriefed_age.port_join
       ) = norpac.debriefed_haul.haul_join

WHERE
    norpac.debriefed_age.nmfs_area
    -- insert location
    AND norpac.debriefed_age.species
    -- insert species

ORDER BY
    norpac.debriefed_age.year,
    month;