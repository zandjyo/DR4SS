SELECT
      CASE 
        WHEN pre1991.foreign_fishing_operation.vessel_type_code in (1,2,3,4) 
        THEN 'TRW'
        WHEN pre1991.foreign_fishing_operation.vessel_type_code in 6 
        THEN 'POT' 
        WHEN pre1991.foreign_fishing_operation.vessel_type_code in (5,7,9,10,11,68,8) 
        THEN 'HAL' 
      END AS GEAR, 
      CONCAT('H',TO_CHAR(pre1991.foreign_haul.haul_join))  AS HAUL_JOIN, 
      TO_CHAR(pre1991.foreign_length.dt, 'MM') AS MONTH,  
      pre1991.foreign_length.year, 
      pre1991.foreign_length.species AS SPECIES,
      pre1991.foreign_spcomp.species_haul_number as NUMB,
      pre1991.foreign_length.cruise,
      pre1991.foreign_length.vessel AS PERMIT,
      pre1991.foreign_length.haul as HAUL,
      pre1991.foreign_spcomp.species_haul_weight as extrapolated_weight,
      CASE 
        WHEN pre1991.foreign_length.SEX IN 'F'
        THEN '1'
        WHEN pre1991.foreign_length.SEX IN 'M'
        THEN '2'
        WHEN pre1991.foreign_length.SEX IN 'U'
        THEN '3'
       END  AS SEX,
      pre1991.foreign_length.size_group AS LENGTH,
      SUM(pre1991.foreign_length.frequency) AS sum_frequency,
      pre1991.foreign_length.dt AS HDAY,
      pre1991.foreign_haul.generic_area AS AREA,
      pre1991.foreign_haul.longitude,
      pre1991.foreign_haul.ADFG_NUMBER AS VES_AKR_ADFG,
      'FOREIGN' AS SOURCE
  FROM
      pre1991.foreign_length
      INNER JOIN pre1991.foreign_haul ON pre1991.foreign_haul.cruise = pre1991.foreign_length.cruise
                                      AND pre1991.foreign_haul.vessel = pre1991.foreign_length.vessel
                                      AND pre1991.foreign_haul.year = pre1991.foreign_length.year
                                      AND pre1991.foreign_haul.haul = pre1991.foreign_length.haul
                                      AND pre1991.foreign_length.haul_join = pre1991.foreign_haul.haul_join
      INNER JOIN pre1991.foreign_fishing_operation ON pre1991.foreign_fishing_operation.cruise = pre1991.foreign_haul.cruise
                                                   AND pre1991.foreign_fishing_operation.vessel = pre1991.foreign_haul.vessel
      INNER JOIN pre1991.foreign_vessel_type ON pre1991.foreign_fishing_operation.vessel_type_code = pre1991.foreign_vessel_type.vessel_type_code
      INNER JOIN pre1991.foreign_spcomp ON pre1991.foreign_length.cruise = pre1991.foreign_spcomp.cruise
                                        AND pre1991.foreign_length.vessel = pre1991.foreign_spcomp.vessel
                                        AND pre1991.foreign_length.haul = pre1991.foreign_spcomp.haul
                                        AND pre1991.foreign_length.year = pre1991.foreign_spcomp.year
                                        AND pre1991.foreign_length.species = pre1991.foreign_spcomp.species
                                        AND pre1991.foreign_length.haul_join = pre1991.foreign_spcomp.haul_join
  WHERE
     pre1991.foreign_length.year > 1976
     AND pre1991.foreign_length.year < 1991
     AND pre1991.foreign_length.species 
         -- insert species
     AND pre1991.foreign_haul.generic_area 
          -- insert region
     AND pre1991.foreign_length.year 
          -- insert syear
     AND pre1991.foreign_length.year 
          -- insert eyear
  GROUP BY
      pre1991.foreign_length.size_group,
      pre1991.foreign_length.sex,
      pre1991.foreign_length.cruise,
      pre1991.foreign_length.vessel,
      pre1991.foreign_length.year,
      pre1991.foreign_length.dt,
      pre1991.foreign_length.haul,
      pre1991.foreign_length.species,
      pre1991.foreign_haul.generic_area,
      pre1991.foreign_haul.longitude,
      pre1991.foreign_fishing_operation.vessel_type_code,
      pre1991.foreign_spcomp.species_haul_number,
      pre1991.foreign_spcomp.species_haul_weight,
      pre1991.foreign_haul.haul_join,
      pre1991.foreign_haul.ADFG_NUMBER
  ORDER BY
      pre1991.foreign_length.dt,
      pre1991.foreign_length.haul,
      pre1991.foreign_length.sex,
      pre1991.foreign_length.size_group