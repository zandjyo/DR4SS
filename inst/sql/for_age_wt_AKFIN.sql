SELECT
        pre1991.foreign_age.species,
        pre1991.foreign_age.year,
        TO_CHAR(pre1991.foreign_age.dt, 'mm') AS month,
        pre1991.foreign_age.dt,
        pre1991.foreign_age.cruise,
        pre1991.foreign_age.vessel,
        pre1991.foreign_fishing_operation.vessel_type_code,
        pre1991.foreign_age.sex,
        pre1991.foreign_age.age,
        pre1991.foreign_age.length,
        pre1991.foreign_age.indiv_weight
    FROM
        pre1991.foreign_age
        INNER JOIN pre1991.foreign_haul ON pre1991.foreign_age.haul_join = pre1991.foreign_haul.haul_join
        AND pre1991.foreign_age.cruise = pre1991.foreign_haul.cruise
        AND pre1991.foreign_age.vessel = pre1991.foreign_haul.vessel
        INNER JOIN pre1991.foreign_fishing_operation ON pre1991.foreign_haul.cruise = pre1991.foreign_fishing_operation.cruise
        AND pre1991.foreign_haul.vessel = pre1991.foreign_fishing_operation.vessel
    WHERE
        pre1991.foreign_age.species 
        -- insert species
        AND pre1991.foreign_haul.generic_area
        -- insert location
    ORDER BY
        pre1991.foreign_age.year,
        pre1991.foreign_age.cruise,
        pre1991.foreign_age.vessel,
        pre1991.foreign_age.length,
        pre1991.foreign_age.indiv_weight
