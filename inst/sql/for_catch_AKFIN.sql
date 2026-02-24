SELECT
      pre1991.foreign_blend.species_name AS SPECIES,
      SUM(pre1991.foreign_blend.blend_tonnage) AS TONS,
      to_char(pre1991.foreign_blend.week_date,'MM') AS MONTH_WED,
      CASE
           WHEN pre1991.foreign_blend.vessel_class LIKE '%POT%'
           THEN 'POT'
           WHEN pre1991.foreign_blend.vessel_class LIKE '%LONG%'
           THEN 'HAL'
           ELSE 'TRW'
      END AS GEAR, 
      pre1991.foreign_blend.yr as YEAR,
      pre1991.foreign_blend.area_number AS AREA,
      'FOREIGN' as SOURCE   
  FROM
      pre1991.foreign_blend
  WHERE
      pre1991.foreign_blend.species_name 
      -- insert species_catch
      AND pre1991.foreign_blend.area_number 
        -- insert area
      AND pre1991.foreign_blend.yr
        -- insert syear
     AND pre1991.foreign_blend.yr
        -- insert eyear
  GROUP BY
      pre1991.foreign_blend.species_name,
      pre1991.foreign_blend.yr,
      to_char(pre1991.foreign_blend.week_date,'MM'),
      pre1991.foreign_blend.area_number,
      CASE WHEN pre1991.foreign_blend.vessel_class LIKE '%POT%' THEN 'POT' 
      WHEN pre1991.foreign_blend.vessel_class LIKE '%LONG%' THEN 'HAL'
      ELSE 'TRW'
      END
ORDER BY
      pre1991.foreign_blend.species_name,
      pre1991.foreign_blend.yr,
      to_char(pre1991.foreign_blend.week_date,'MM'),
      pre1991.foreign_blend.area_number,
      CASE WHEN pre1991.foreign_blend.vessel_class LIKE '%POT%' THEN 'POT' 
      WHEN pre1991.foreign_blend.vessel_class LIKE '%LONG%' THEN 'HAL'
      ELSE 'TRW'
      END