{{
  config({    
    "materialized": "ephemeral",
    "database": "tanmay",
    "schema": "default2"
  })
}}

WITH MultiFieldFormula_399 AS (

  SELECT *
  
  FROM {{ ref('facility_master_wf_1___MultiFieldFormula_399')}}

),

AlteryxSelect_53 AS (

  SELECT 
    contact_id AS contact_id,
    city_town_village AS city_town_village,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name
  
  FROM MultiFieldFormula_399 AS in0

),

Filter_221 AS (

  SELECT * 
  
  FROM AlteryxSelect_53 AS in0
  
  WHERE (NOT(latitude IS NULL))

),

Summarize_239 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((latitude IS NULL) OR (CAST(latitude AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_lat,
    COUNT(
      (
        CASE
          WHEN ((longitude IS NULL) OR (CAST(longitude AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_long,
    latitude1 AS latitude1,
    longitude1 AS longitude1
  
  FROM Filter_221 AS in0
  
  GROUP BY 
    latitude1, longitude1

),

Filter_240_reject AS (

  SELECT * 
  
  FROM Summarize_239 AS in0
  
  WHERE ((NOT((Count_lat = 1) AND (Count_long = 1))) OR (((Count_lat = 1) AND (Count_long = 1)) IS NULL))

),

Join_248_inner AS (

  SELECT 
    in1.latitude1 AS Right_latitude1,
    in1.longitude1 AS Right_longitude1,
    in0.*,
    in1.* EXCEPT (`latitude1`, `longitude1`)
  
  FROM Filter_221 AS in0
  INNER JOIN Filter_240_reject AS in1
     ON ((in0.latitude1 = in1.latitude1) AND (in0.longitude1 = in1.longitude1))

),

AlteryxSelect_252 AS (

  SELECT 
    contact_id AS contact_id,
    city_town_village AS city_town_village,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name
  
  FROM Join_248_inner AS in0

),

AlteryxSelect_474 AS (

  SELECT 
    latitude1 AS lat,
    longitude1 AS lot,
    * EXCEPT (`contact_id`, `latitude1`, `longitude1`)
  
  FROM AlteryxSelect_252 AS in0

),

Cleanse_476 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_474'], 
      [
        { "name": "lat", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "org_name", "dataType": "String" }
      ], 
      'makeTitleCase', 
      ['city_town_village'], 
      true, 
      '', 
      true, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Filter_478 AS (

  SELECT * 
  
  FROM Cleanse_476 AS in0
  
  WHERE (NOT(org_name IS NULL))

),

Filter_475 AS (

  SELECT * 
  
  FROM Filter_478 AS in0
  
  WHERE (
          (
            NOT(
              org_name = 'ABC')
          ) OR (org_name IS NULL)
        )

),

Summarize_480 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((city_town_village IS NULL) OR (CAST(city_town_village AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_num,
    latitude AS latitude,
    longitude AS longitude
  
  FROM Filter_475 AS in0
  
  GROUP BY 
    latitude, longitude

),

Filter_481 AS (

  SELECT * 
  
  FROM Summarize_480 AS in0
  
  WHERE (Count_num > 1)

),

Join_484_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`Count_num`, `latitude`, `longitude`)
  
  FROM Filter_475 AS in0
  INNER JOIN Filter_481 AS in1
     ON ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))

),

Summarize_486 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((city_town_village IS NULL) OR (CAST(city_town_village AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    city_town_village AS city_town_village,
    latitude AS latitude,
    longitude AS longitude
  
  FROM Join_484_inner AS in0
  
  GROUP BY 
    city_town_village, latitude, longitude

),

Summarize_487 AS (

  SELECT 
    *,
    MAX(Count) OVER (PARTITION BY latitude, longitude ORDER BY 1 ASC NULLS FIRST) AS Max_Count
  
  FROM Summarize_486 AS in0

),

Summarize_487_filter AS (

  SELECT * 
  
  FROM Summarize_487 AS in0
  
  WHERE (Max_Count = Count)

),

Join_488_inner_formula_0 AS (

  SELECT 
    latitude AS latitude,
    longitude AS longitude,
    city_town_village AS Right_city_town_village,
    * EXCEPT (`latitude`, `longitude`)
  
  FROM Summarize_487_filter AS in0

),

Sample_494 AS (

  {{
    prophecy_basics.Sample(
      ['Join_488_inner_formula_0'], 
      '[{"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "Right_city_town_village", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "city_town_village", "dataType": "String"}, {"name": "Max_Count", "dataType": "Bigint"}]', 
      'sampleGroup', 
      ['latitude', 'longitude'], 
      1002, 
      'firstN', 
      1
    )
  }}

),

Join_491_inner AS (

  SELECT 
    in1.city_town_village AS city_town_village,
    in0.lat AS lat,
    in0.lot AS lot,
    in0.latitude AS latitude,
    in0.longitude AS longitude,
    in0.* EXCEPT (`city_town_village`, `lat`, `lot`, `latitude`, `longitude`),
    in1.* EXCEPT (`Right_city_town_village`, `latitude`, `longitude`, `city_town_village`)
  
  FROM Filter_475 AS in0
  INNER JOIN Sample_494 AS in1
     ON ((in0.latitude = CAST(in1.latitude AS DOUBLE)) AND (in0.longitude = CAST(in1.longitude AS DOUBLE)))

),

Filter_481_reject AS (

  SELECT * 
  
  FROM Summarize_480 AS in0
  
  WHERE (
          (
            NOT(
              Count_num > 1)
          ) OR ((Count_num > 1) IS NULL)
        )

),

Join_482_inner AS (

  SELECT 
    in1.city_town_village AS city_town_village,
    in1.lat AS lat,
    in1.lot AS lot,
    in0.latitude AS latitude,
    in0.longitude AS longitude,
    in0.* EXCEPT (`latitude`, `longitude`, `Count_num`),
    in1.* EXCEPT (`city_town_village`, `lat`, `lot`, `latitude`, `longitude`)
  
  FROM Filter_481_reject AS in0
  INNER JOIN Filter_475 AS in1
     ON ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))

),

Union_515 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_482_inner', 'Join_491_inner'], 
      [
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}]', 
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "Max_Count", "dataType": "Bigint"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_498 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((city_town_village IS NULL) OR (CAST(city_town_village AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS Count_num,
    lat AS lat,
    lot AS lot
  
  FROM Union_515 AS in0
  
  GROUP BY 
    lat, lot

),

Filter_499_reject AS (

  SELECT * 
  
  FROM Summarize_498 AS in0
  
  WHERE (
          (
            NOT(
              Count_num > 1)
          ) OR ((Count_num > 1) IS NULL)
        )

),

Join_500_inner AS (

  SELECT 
    in1.city_town_village AS city_town_village,
    in1.lat AS lat,
    in1.lot AS lot,
    in1.latitude AS latitude,
    in1.longitude AS longitude,
    in1.org_name AS org_name,
    in0.* EXCEPT (`Count_num`, `lat`, `lot`),
    in1.* EXCEPT (`city_town_village`, `lat`, `lot`, `latitude`, `longitude`, `org_name`)
  
  FROM Filter_499_reject AS in0
  INNER JOIN Union_515 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

Filter_499 AS (

  SELECT * 
  
  FROM Summarize_498 AS in0
  
  WHERE (Count_num > 1)

),

Join_502_inner AS (

  SELECT 
    in1.lat AS Right_lat,
    in1.lot AS Right_lot,
    in0.*,
    in1.* EXCEPT (`Count_num`, `lat`, `lot`)
  
  FROM Union_515 AS in0
  INNER JOIN Filter_499 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

Summarize_504 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN ((city_town_village IS NULL) OR (CAST(city_town_village AS string) = ''))
            THEN NULL
          ELSE 1
        END
      )) AS `Count`,
    city_town_village AS city_town_village,
    lat AS lat,
    lot AS lot
  
  FROM Join_502_inner AS in0
  
  GROUP BY 
    city_town_village, lat, lot

),

Summarize_505 AS (

  SELECT 
    *,
    MAX(Count) OVER (PARTITION BY lat, lot ORDER BY 1 ASC NULLS FIRST) AS Max_Count
  
  FROM Summarize_504 AS in0

),

Summarize_505_filter AS (

  SELECT * 
  
  FROM Summarize_505 AS in0
  
  WHERE (Max_Count = Count)

),

Join_506_inner_formula_0 AS (

  SELECT 
    lat AS lat,
    lot AS lot,
    city_town_village AS Right_city_town_village,
    * EXCEPT (`lot`, `lat`)
  
  FROM Summarize_505_filter AS in0

),

Sample_512 AS (

  {{
    prophecy_basics.Sample(
      ['Join_506_inner_formula_0'], 
      '[{"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "Right_city_town_village", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "city_town_village", "dataType": "String"}, {"name": "Max_Count", "dataType": "Bigint"}]', 
      'sampleGroup', 
      ['lat', 'lot'], 
      1002, 
      'firstN', 
      1
    )
  }}

),

Join_509_inner AS (

  SELECT 
    in0.latitude AS latitude,
    in0.longitude AS longitude,
    in1.city_town_village AS city_town_village,
    in0.org_name AS org_name,
    in0.lot AS lot,
    in0.lat AS lat
  
  FROM Union_515 AS in0
  INNER JOIN Sample_512 AS in1
     ON ((in0.lat = CAST(in1.lat AS DOUBLE)) AND (in0.lot = CAST(in1.lot AS DOUBLE)))

),

Filter_475_reject AS (

  SELECT * 
  
  FROM Filter_478 AS in0
  
  WHERE (
          (
            NOT((
              NOT(
                org_name = 'ABC')
            ) OR (org_name IS NULL))
          )
          OR (
               (
                 (
                   NOT(
                     org_name = 'ABC')
                 ) OR (org_name IS NULL)
               ) IS NULL
             )
        )

),

Unique_521 AS (

  SELECT * 
  
  FROM Filter_475_reject AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY latitude, longitude ORDER BY latitude, longitude) = 1

),

Union_516 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_500_inner', 'Join_509_inner', 'Unique_521'], 
      [
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "Max_Count", "dataType": "Bigint"}]', 
        '[{"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "org_name", "dataType": "String"}, {"name": "lot", "dataType": "Double"}, {"name": "lat", "dataType": "Double"}]', 
        '[{"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

AlteryxSelect_617 AS (

  SELECT * EXCEPT (`city_town_village`, `latitude`, `longitude`, `org_name`)
  
  FROM Union_516 AS in0

),

Unique_619 AS (

  SELECT * 
  
  FROM AlteryxSelect_617 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot ORDER BY lat, lot) = 1

),

RecordID_620 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_619'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Formula_621_0 AS (

  SELECT 
    CAST((RecordID - 1) AS INTEGER) AS RecordID_Rowplus1,
    *
  
  FROM RecordID_620 AS in0

),

AlteryxSelect_623 AS (

  SELECT 
    RecordID AS RecordID_old,
    * EXCEPT (`RecordID`)
  
  FROM Formula_621_0 AS in0

),

AlteryxSelect_622 AS (

  SELECT * EXCEPT (`RecordID_Rowplus1`)
  
  FROM Formula_621_0 AS in0

),

Join_624_left_UnionLeftOuter AS (

  SELECT 
    in0.RecordID AS RecordID,
    in0.lat AS lat,
    in0.lot AS lot,
    (
      CASE
        WHEN (in0.RecordID = in1.RecordID_Rowplus1)
          THEN in1.RecordID_Rowplus1
        ELSE NULL
      END
    ) AS Right_RecordID,
    (
      CASE
        WHEN (in0.RecordID = in1.RecordID_Rowplus1)
          THEN in1.lat
        ELSE NULL
      END
    ) AS Right_lat,
    (
      CASE
        WHEN (in0.RecordID = in1.RecordID_Rowplus1)
          THEN in1.lot
        ELSE NULL
      END
    ) AS Right_lot,
    (
      CASE
        WHEN (in0.RecordID = in1.RecordID_Rowplus1)
          THEN in1.RecordID_old
        ELSE NULL
      END
    ) AS RecordID_Rowplus1,
    in0.* EXCEPT (`RecordID`, `lat`, `lot`, `Count`, `Max_Count`),
    in1.* EXCEPT (`RecordID_old`, `RecordID_Rowplus1`, `lat`, `lot`)
  
  FROM AlteryxSelect_622 AS in0
  LEFT JOIN AlteryxSelect_623 AS in1
     ON (in0.RecordID = in1.RecordID_Rowplus1)

),

Unique_612 AS (

  SELECT * 
  
  FROM Join_624_left_UnionLeftOuter AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY RecordID, lat, lot, RecordID_Rowplus1, Right_RecordID, Right_lat, Right_lot ORDER BY RecordID, lat, lot, RecordID_Rowplus1, Right_RecordID, Right_lat, Right_lot) = 1

),

Formula_610_0 AS (

  SELECT 
    CAST((
      CONCAT(
        '{ "type": "Point", "coordinates": [ ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(lat AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ',', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(lot AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        '] }')
    ) AS string) AS store_p,
    CAST((
      CONCAT(
        '{ "type": "Point", "coordinates": [ ', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(Right_lat AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ',', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(Right_lot AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        '] }')
    ) AS string) AS custom_p,
    *
  
  FROM Unique_612 AS in0

),

CreatePoints_602_cast_lonlat AS (

  SELECT 
    CAST(Right_lot AS DOUBLE) AS Right_lot,
    CAST(Right_lat AS DOUBLE) AS Right_lat,
    * EXCEPT (`Right_lot`, `Right_lat`)
  
  FROM Formula_610_0 AS in0

),

CreatePoints_602 AS (

  {{
    DatabricksSqlSpatial.CreatePoint(
      'CreatePoints_602_cast_lonlat', 
      [['Right_lot', 'Right_lat', 'Centroid']]
    )
  }}

),

CreatePoints_601_cast_lonlat AS (

  SELECT 
    CAST(lot AS DOUBLE) AS lot,
    CAST(lat AS DOUBLE) AS lat,
    * EXCEPT (`lot`, `lat`)
  
  FROM Formula_610_0 AS in0

),

CreatePoints_601 AS (

  {{ DatabricksSqlSpatial.CreatePoint('CreatePoints_601_cast_lonlat', [['lot', 'lat', 'Centroid']]) }}

),

Join_603_inner AS (

  SELECT 
    in1.store_p AS Right_store_p,
    in1.custom_p AS Right_custom_p,
    in1.Centroid AS Right_Centroid,
    in0.* EXCEPT (`Max_Count`, `Count`),
    in1.* EXCEPT (`RecordID`, 
    `lat`, 
    `lot`, 
    `RecordID_Rowplus1`, 
    `Right_RecordID`, 
    `Right_lat`, 
    `Right_lot`, 
    `store_p`, 
    `custom_p`, 
    `Centroid`)
  
  FROM CreatePoints_601 AS in0
  INNER JOIN CreatePoints_602 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

Distance_605 AS (

  {{
    DatabricksSqlSpatial.Distance(
      'Join_603_inner', 
      'Centroid', 
      'Right_Centroid', 
      'point', 
      'point', 
      true, 
      'mtr', 
      true, 
      true, 
      [
        'Right_store_p', 
        'Right_custom_p', 
        'Right_Centroid', 
        'lot', 
        'lat', 
        'store_p', 
        'custom_p', 
        'RecordID', 
        'Right_RecordID', 
        'Right_lat', 
        'Right_lot', 
        'RecordID_Rowplus1', 
        'Centroid', 
        'Count', 
        'Max_Count'
      ]
    )
  }}

),

AlteryxSelect_607 AS (

  SELECT 
    RecordID AS RecordID,
    lat AS lat,
    lot AS lot,
    RecordID_Rowplus1 AS RecordID_Rowplus1,
    Right_RecordID AS Right_RecordID,
    Right_lat AS Right_lat,
    Right_lot AS Right_lot,
    DistanceMeters AS DistanceMeters,
    store_p AS store_p,
    custom_p AS custom_p,
    Centroid AS Centroid,
    CAST(NULL AS string) AS city_town_village,
    CAST(NULL AS string) AS contact_id
  
  FROM Distance_605 AS in0

),

Filter_627 AS (

  SELECT * 
  
  FROM AlteryxSelect_607 AS in0
  
  WHERE (DistanceMeters >= 1000)

),

AlteryxSelect_631 AS (

  SELECT 
    RecordID AS RecordID,
    lat AS lat,
    lot AS lot,
    city_town_village AS city_town_village,
    contact_id AS contact_id
  
  FROM Filter_627 AS in0

),

Union_848_reformat_0 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    city_town_village AS city_town_village,
    contact_id AS contact_id,
    CAST(lat AS string) AS lat,
    CAST(lot AS string) AS lot
  
  FROM AlteryxSelect_631 AS in0

),

AlteryxSelect_846 AS (

  SELECT 
    RecordID_Rowplus1 AS RecordID,
    Right_lat AS lat,
    Right_lot AS lot,
    city_town_village AS city_town_village,
    contact_id AS contact_id
  
  FROM Filter_627 AS in0

),

Union_848_reformat_1 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    city_town_village AS city_town_village,
    contact_id AS contact_id,
    CAST(lat AS string) AS lat,
    CAST(lot AS string) AS lot
  
  FROM AlteryxSelect_846 AS in0

),

Union_848 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_848_reformat_0', 'Union_848_reformat_1'], 
      [
        '[{"name": "RecordID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "contact_id", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]', 
        '[{"name": "RecordID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "contact_id", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_850 AS (

  SELECT * 
  
  FROM Union_848 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot, RecordID ORDER BY lat, lot, RecordID) = 1

),

Union_632_reformat_1 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    CAST(city_town_village AS string) AS city_town_village,
    CAST(contact_id AS string) AS contact_id,
    lat AS lat,
    lot AS lot
  
  FROM Unique_850 AS in0

),

Filter_627_reject AS (

  SELECT * 
  
  FROM AlteryxSelect_607 AS in0
  
  WHERE (
          (
            NOT(
              DistanceMeters >= 1000)
          ) OR ((DistanceMeters >= 1000) IS NULL)
        )

),

AlteryxSelect_628 AS (

  SELECT 
    RecordID AS RecordID,
    lat AS lat,
    lot AS lot,
    city_town_village AS city_town_village,
    contact_id AS contact_id
  
  FROM Filter_627_reject AS in0

),

Union_630_reformat_0 AS (

  SELECT 
    RecordID AS RecordID,
    city_town_village AS city_town_village,
    contact_id AS contact_id,
    CAST(lat AS string) AS lat,
    CAST(lot AS string) AS lot
  
  FROM AlteryxSelect_628 AS in0

),

AlteryxSelect_629 AS (

  SELECT 
    RecordID AS RecordID,
    Right_lat AS lat,
    Right_lot AS lot,
    city_town_village AS city_town_village,
    contact_id AS contact_id
  
  FROM Filter_627_reject AS in0

),

Union_630_reformat_1 AS (

  SELECT 
    RecordID AS RecordID,
    city_town_village AS city_town_village,
    contact_id AS contact_id,
    CAST(lat AS string) AS lat,
    CAST(lot AS string) AS lot
  
  FROM AlteryxSelect_629 AS in0

),

Union_630 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_630_reformat_0', 'Union_630_reformat_1'], 
      [
        '[{"name": "RecordID", "dataType": "Integer"}, {"name": "city_town_village", "dataType": "String"}, {"name": "contact_id", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]', 
        '[{"name": "RecordID", "dataType": "Integer"}, {"name": "city_town_village", "dataType": "String"}, {"name": "contact_id", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_840 AS (

  SELECT 
    MAX(RecordID) AS RecordID,
    lat AS lat,
    lot AS lot
  
  FROM Union_630 AS in0
  
  GROUP BY 
    lat, lot

),

Union_632_reformat_0 AS (

  SELECT 
    CAST(RecordID AS string) AS RecordID,
    lat AS lat,
    lot AS lot
  
  FROM Summarize_840 AS in0

),

Union_632 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_632_reformat_0', 'Union_632_reformat_1'], 
      [
        '[{"name": "RecordID", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]', 
        '[{"name": "RecordID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "contact_id", "dataType": "String"}, {"name": "lat", "dataType": "String"}, {"name": "lot", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_791 AS (

  SELECT 
    MIN(RecordID) AS RecordID,
    lat AS lat,
    lot AS lot
  
  FROM Union_632 AS in0
  
  GROUP BY 
    lat, lot

),

Unique_851 AS (

  SELECT * 
  
  FROM Summarize_791 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot, RecordID ORDER BY lat, lot, RecordID) = 1

),

Filter_657 AS (

  SELECT * 
  
  FROM Unique_851 AS in0
  
  WHERE (NOT(lat IS NULL))

),

Unique_987 AS (

  SELECT * 
  
  FROM Union_516 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village, lat, lot, latitude, longitude, org_name ORDER BY city_town_village, lat, lot, latitude, longitude, org_name) = 1

),

Join_993_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`lat`, `lot`)
  
  FROM Unique_987 AS in0
  INNER JOIN Filter_657 AS in1
     ON ((in0.lat = CAST(in1.lat AS DOUBLE)) AND (in0.lot = CAST(in1.lot AS DOUBLE)))

),

Filter_764 AS (

  SELECT * 
  
  FROM MultiFieldFormula_399 AS in0
  
  WHERE (NOT(latitude IS NULL))

),

Join_642_inner AS (

  SELECT 
    in0.city_town_village AS city_town_village_old,
    in1.latitude AS Right_latitude,
    in1.longitude AS Right_longitude,
    in1.org_name AS Right_org_name,
    in0.* EXCEPT (`city_town_village`),
    in1.* EXCEPT (`lat`, `lot`, `latitude`, `longitude`, `org_name`)
  
  FROM Filter_764 AS in0
  INNER JOIN Join_993_inner AS in1
     ON ((in0.latitude1 = in1.lat) AND (in0.longitude1 = in1.lot))

),

AlteryxSelect_659 AS (

  SELECT 
    city_town_village_old AS city_town_village_old,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name,
    city_town_village AS city_town_village,
    RecordID AS GroupID
  
  FROM Join_642_inner AS in0

),

Unique_661 AS (

  SELECT * 
  
  FROM AlteryxSelect_659 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village_old, latitude1, longitude1, latitude, longitude, org_name, city_town_village, GroupID ORDER BY city_town_village_old, latitude1, longitude1, latitude, longitude, org_name, city_town_village, GroupID) = 1

),

AlteryxSelect_858 AS (

  SELECT 
    city_town_village_old AS city_town_village_old,
    latitude1 AS lat,
    longitude1 AS lot,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS organization_name,
    city_town_village AS city_town_village,
    GroupID AS GroupID
  
  FROM Unique_661 AS in0

),

Cleanse_860 AS (

  {{
    prophecy_basics.DataCleansing(
      ['AlteryxSelect_858'], 
      [
        { "name": "city_town_village_old", "dataType": "String" }, 
        { "name": "lat", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "GroupID", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['organization_name'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Unique_859 AS (

  SELECT * 
  
  FROM Cleanse_860 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village_old, lat, lot, latitude, longitude, organization_name, city_town_village, GroupID ORDER BY city_town_village_old, lat, lot, latitude, longitude, organization_name, city_town_village, GroupID) = 1

),

AlteryxSelect_957 AS (

  SELECT 
    lat AS lat,
    lot AS lot,
    GroupID AS GroupID
  
  FROM Unique_859 AS in0

),

Unique_959 AS (

  SELECT * 
  
  FROM AlteryxSelect_957 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot, GroupID ORDER BY lat, lot, GroupID) = 1

),

AlteryxSelect_958 AS (

  SELECT * EXCEPT (`GroupID`)
  
  FROM Unique_859 AS in0

),

Unique_960 AS (

  SELECT * 
  
  FROM AlteryxSelect_958 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village_old, lat, lot, latitude, longitude, organization_name, city_town_village ORDER BY city_town_village_old, lat, lot, latitude, longitude, organization_name, city_town_village) = 1

),

Join_863_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`lat`, `lot`)
  
  FROM Unique_959 AS in0
  INNER JOIN Unique_960 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

Formula_864_0 AS (

  SELECT 
    CAST((
      CONCAT(
        '(', 
        (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')), 
        ')')
    ) AS string) AS EXP,
    *
  
  FROM Join_863_inner AS in0

),

Summarize_866 AS (

  SELECT 
    COUNT(DISTINCT organization_name) AS CountDistinctNonNull_organization_name,
    EXP AS EXP,
    GroupID AS GroupID
  
  FROM Formula_864_0 AS in0
  
  GROUP BY 
    EXP, GroupID

),

Filter_867 AS (

  SELECT * 
  
  FROM Summarize_866 AS in0
  
  WHERE (CountDistinctNonNull_organization_name > 1)

),

Macro_1004 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file ../../../../../AppData/Local/Temp/1/Staging/b0fb5853-3511-4c0f-8789-a3ffc1968604/_externals/1/Dynamic Filter_v2.yxmc to resolve it.'
    )
  }}

),

Union_862_reformat_0 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    CAST(lat AS DOUBLE) AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(lot AS DOUBLE) AS lot,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    organization_name AS organization_name
  
  FROM Macro_1004 AS in0

),

Filter_867_reject AS (

  SELECT * 
  
  FROM Summarize_866 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_organization_name > 1)
          )
          OR ((CountDistinctNonNull_organization_name > 1) IS NULL)
        )

),

Join_869_inner AS (

  SELECT 
    in0.* EXCEPT (`EXP`, `CountDistinctNonNull_organization_name`),
    in1.* EXCEPT (`GroupID`)
  
  FROM Filter_867_reject AS in0
  INNER JOIN Join_863_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Formula_870_0 AS (

  SELECT 
    CAST(organization_name AS string) AS new_organization_name_fuzzy_unique,
    *
  
  FROM Join_869_inner AS in0

),

Union_862_reformat_1 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    CAST(lat AS DOUBLE) AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(lot AS DOUBLE) AS lot,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    organization_name AS organization_name
  
  FROM Formula_870_0 AS in0

),

Union_862 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_862_reformat_1', 'Union_862_reformat_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}]', 
        '[{"name": "GroupID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Formula_961_0 AS (

  SELECT 
    CAST((REGEXP_REPLACE(new_organization_name_fuzzy_unique, '"', '')) AS string) AS new_organization_name_fuzzy_unique,
    * EXCEPT (`new_organization_name_fuzzy_unique`)
  
  FROM Union_862 AS in0

),

Filter_879 AS (

  SELECT * 
  
  FROM Formula_961_0 AS in0
  
  WHERE (NOT(longitude IS NULL))

),

Cleanse_886 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Filter_879'], 
      [
        { "name": "new_organization_name_fuzzy_unique", "dataType": "String" }, 
        { "name": "GroupID", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "city_town_village_old", "dataType": "String" }, 
        { "name": "lat", "dataType": "Double" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "organization_name", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['organization_name'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Filter_887 AS (

  SELECT * 
  
  FROM Cleanse_886 AS in0
  
  WHERE ((LENGTH(new_organization_name_fuzzy_unique)) > 0)

),

Summarize_878 AS (

  SELECT 
    COUNT(
      (
        CASE
          WHEN CAST((new_organization_name_fuzzy_unique IS NULL) AS BOOLEAN)
            THEN ''
          ELSE new_organization_name_fuzzy_unique
        END
      )) AS `Count`,
    new_organization_name_fuzzy_unique AS new_organization_name_fuzzy_unique,
    GroupID AS GroupID
  
  FROM Filter_887 AS in0
  
  GROUP BY 
    new_organization_name_fuzzy_unique, GroupID

),

RegEx_929 AS (

  {{
    prophecy_basics.Regex(
      ['Summarize_878'], 
      [
        {
          'columnName': 'regex_col1', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[A-Z_0-9](\\s|:|,|-|l{1}|\\+|/|\\.).*$)'
        }, 
        {
          'columnName': 'regex_col2', 
          'dataType': 'string', 
          'rgxExpression': '(^(\\\\u{1})(\\s|-|\\.|\\+|,).*$)'
        }, 
        {
          'columnName': 'regex_col3', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\d).(-|\\s|.|,|/+|(?:^\\w+)).*$)'
        }, 
        { 'columnName': 'regex_col4', 'dataType': 'string', 'rgxExpression': '((?:^\\w+)$)' }, 
        {
          'columnName': 'regex_col5', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[a-z_0-9](:|,|-|\\+|/).*$)'
        }
      ], 
      '[{"name": "Count", "dataType": "Bigint"}, {"name": "new_organization_name_fuzzy_unique", "dataType": "String"}, {"name": "GroupID", "dataType": "String"}]', 
      'new_organization_name_fuzzy_unique', 
      '((?:^\w+)[A-Z_0-9](\s|:|,|-|l{1}|\+|/|\.).*$)|(^(\\u{1})(\s|-|\.|\+|,).*$)|((?:^\d).(-|\s|.|,|/+|(?:^\w+)).*$)|((?:^\w+)$)|((?:^\w+)[a-z_0-9](:|,|-|\+|/).*$)', 
      'match', 
      false, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'starts_with_uppercase_letter', 
      false, 
      '_replaced'
    )
  }}

),

RegEx_929_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_929 AS in0

),

Filter_928 AS (

  SELECT * 
  
  FROM RegEx_929_typeCastGem AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (length(new_organization_name_fuzzy_unique) < 3)
                                                                                            OR coalesce(contains(lower(new_organization_name_fuzzy_unique), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_organization_name_fuzzy_unique, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                          )
                                                          OR (
                                                               substring(
                                                                 CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                                 ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                                                                 2) = ' W'
                                                             )
                                                        )
                                                        OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARKWAY')), false)
                                        )
                                        OR (
                                             substring(
                                               CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                               ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                               6) = 'STREET'
                                           )
                                      )
                                      OR (
                                           substring(
                                             CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                             ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                             4) = 'ROAD'
                                         )
                                    )
                                    OR (
                                         substring(
                                           CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                           ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                           6) = 'AVENUE'
                                       )
                                  )
                                  OR (
                                       substring(
                                         CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                         ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                         3) = 'BLV'
                                     )
                                )
                                OR (
                                     substring(
                                       CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                       ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                       4) = 'BLVD'
                                   )
                              )
                              OR (
                                   substring(
                                     CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                     ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                     5) = 'PLAZA'
                                 )
                            )
                            OR (
                                 substring(
                                   CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                   ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                   5) = 'DRIVE'
                               )
                          )
                          OR (
                               substring(
                                 CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                 ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                 4) = ' WAY'
                             )
                        )
                        OR (
                             substring(
                               CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                               ((length(new_organization_name_fuzzy_unique) - 8) + 1), 
                               8) = ' HIGHWAY'
                           )
                      )
                      OR (
                           substring(
                             CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                             ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                             3) = 'AVE'
                         )
                    )
                    OR (
                         substring(
                           CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                           ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                           3) = 'STR'
                       )
                  )
                  OR (
                       substring(
                         CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                         ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                         2) = 'DR'
                     )
                )
                OR (
                     substring(
                       CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                       ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                       3) = ' ST'
                   )
              )
              OR (
                   substring(
                     CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                     ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                     3) = ' RD'
                 )
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         (
                                                           (
                                                             (
                                                               (
                                                                 (
                                                                   (
                                                                     (
                                                                       (
                                                                         (
                                                                           (
                                                                             (
                                                                               (
                                                                                 (
                                                                                   (
                                                                                     (
                                                                                       (
                                                                                         (
                                                                                           (
                                                                                             (
                                                                                               (length(new_organization_name_fuzzy_unique) < 3)
                                                                                               OR coalesce(contains(lower(new_organization_name_fuzzy_unique), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_organization_name_fuzzy_unique, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                             )
                                                             OR (
                                                                  substring(
                                                                    CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                                    ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                                                                    2) = ' W'
                                                                )
                                                           )
                                                           OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_organization_name_fuzzy_unique)), lower(' PARKWAY')), false)
                                           )
                                           OR (
                                                substring(
                                                  CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                  ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                                  6) = 'STREET'
                                              )
                                         )
                                         OR (
                                              substring(
                                                CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                                ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                                4) = 'ROAD'
                                            )
                                       )
                                       OR (
                                            substring(
                                              CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                              ((length(new_organization_name_fuzzy_unique) - 6) + 1), 
                                              6) = 'AVENUE'
                                          )
                                     )
                                     OR (
                                          substring(
                                            CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                            ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                            3) = 'BLV'
                                        )
                                   )
                                   OR (
                                        substring(
                                          CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                          ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                          4) = 'BLVD'
                                      )
                                 )
                                 OR (
                                      substring(
                                        CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                        ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                        5) = 'PLAZA'
                                    )
                               )
                               OR (
                                    substring(
                                      CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                      ((length(new_organization_name_fuzzy_unique) - 5) + 1), 
                                      5) = 'DRIVE'
                                  )
                             )
                             OR (
                                  substring(
                                    CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                    ((length(new_organization_name_fuzzy_unique) - 4) + 1), 
                                    4) = ' WAY'
                                )
                           )
                           OR (
                                substring(
                                  CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                  ((length(new_organization_name_fuzzy_unique) - 8) + 1), 
                                  8) = ' HIGHWAY'
                              )
                         )
                         OR (
                              substring(
                                CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                                ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                                3) = 'AVE'
                            )
                       )
                       OR (
                            substring(
                              CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                              ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                              3) = 'STR'
                          )
                     )
                     OR (
                          substring(
                            CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                            ((length(new_organization_name_fuzzy_unique) - 2) + 1), 
                            2) = 'DR'
                        )
                   )
                   OR (
                        substring(
                          CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                          ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                          3) = ' ST'
                      )
                 )
                 OR (
                      substring(
                        CAST(upper(new_organization_name_fuzzy_unique) AS STRING), 
                        ((length(new_organization_name_fuzzy_unique) - 3) + 1), 
                        3) = ' RD'
                    )
               ))
        )

),

Summarize_883 AS (

  SELECT 
    MAX(Count) AS Max_Count,
    GroupID AS GroupID
  
  FROM Filter_928 AS in0
  
  GROUP BY GroupID

),

Filter_888_reject AS (

  SELECT * 
  
  FROM Summarize_883 AS in0
  
  WHERE (
          (
            NOT(
              Max_Count = CAST('1' AS DOUBLE))
          ) OR ((Max_Count = CAST('1' AS DOUBLE)) IS NULL)
        )

),

Join_890_inner AS (

  SELECT 
    in1.new_organization_name_fuzzy_unique AS new_org_name_alteryx,
    in1.Count AS `Count`,
    in1.GroupID AS GroupID
  
  FROM Filter_888_reject AS in0
  INNER JOIN Summarize_878 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.Max_Count = in1.Count))

),

Summarize_917 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Join_890_inner AS in0
  
  GROUP BY GroupID

),

Filter_918_reject AS (

  SELECT * 
  
  FROM Summarize_917 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Join_913_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_918_reject AS in0
  INNER JOIN Join_890_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_918 AS (

  SELECT * 
  
  FROM Summarize_917 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Join_916_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_918 AS in0
  INNER JOIN Join_890_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

RegEx_926 AS (

  {{
    prophecy_basics.Regex(
      ['Join_916_inner'], 
      [
        {
          'columnName': 'regex_col1', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[A-Z_0-9](\\s|:|,|-|l{1}|\\+|/|\\.).*$)'
        }, 
        {
          'columnName': 'regex_col2', 
          'dataType': 'string', 
          'rgxExpression': '(^(\\\\u{1})(\\s|-|\\.|\\+|,).*$)'
        }, 
        {
          'columnName': 'regex_col3', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\d).(-|\\s|.|,|/+|(?:^\\w+)).*$)'
        }, 
        { 'columnName': 'regex_col4', 'dataType': 'string', 'rgxExpression': '((?:^\\w+)$)' }, 
        {
          'columnName': 'regex_col5', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[a-z_0-9](:|,|-|\\+|/).*$)'
        }
      ], 
      '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}]', 
      'new_org_name_alteryx', 
      '((?:^\w+)[A-Z_0-9](\s|:|,|-|l{1}|\+|/|\.).*$)|(^(\\u{1})(\s|-|\.|\+|,).*$)|((?:^\d).(-|\s|.|,|/+|(?:^\w+)).*$)|((?:^\w+)$)|((?:^\w+)[a-z_0-9](:|,|-|\+|/).*$)', 
      'match', 
      false, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'starts_with_uppercase_letter', 
      false, 
      '_replaced'
    )
  }}

),

RegEx_926_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_926 AS in0

),

Filter_915 AS (

  SELECT * 
  
  FROM RegEx_926_typeCastGem AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (length(new_org_name_alteryx) < 3)
                                                                                        OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                      )
                                                                                      OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                    )
                                                                                    OR (starts_with_uppercase_letter = true)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                          )
                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                        )
                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                      )
                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
            )
            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
          )
          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
        )

),

Summarize_920 AS (

  SELECT 
    first(new_org_name_alteryx) AS `1_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_915 AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_915_reject AS (

  SELECT * 
  
  FROM RegEx_926_typeCastGem AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (length(new_org_name_alteryx) < 3)
                                                                                            OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                          )
                                                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                        )
                                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                      )
                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         (
                                                           (
                                                             (
                                                               (
                                                                 (
                                                                   (
                                                                     (
                                                                       (
                                                                         (
                                                                           (
                                                                             (
                                                                               (
                                                                                 (
                                                                                   (
                                                                                     (
                                                                                       (
                                                                                         (
                                                                                           (
                                                                                             (
                                                                                               (length(new_org_name_alteryx) < 3)
                                                                                               OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                             )
                                                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                           )
                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                           )
                                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                         )
                                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                       )
                                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                     )
                                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                   )
                                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                                 )
                                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                               )
                               OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                             )
                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                           )
                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                         )
                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                       )
                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                     )
                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                   )
                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
                 )
                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
               ))
        )

),

Summarize_919 AS (

  SELECT 
    first(new_org_name_alteryx) AS `2_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_915_reject AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_921 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_919', 'Summarize_920'], 
      [
        '[{"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]', 
        '[{"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

MultiRowFormula_924_window AS (

  SELECT 
    *,
    lead(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lead1`,
    lag(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lag1`
  
  FROM Union_921 AS in0

),

MultiRowFormula_924_0 AS (

  SELECT 
    CASE
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lead1`) = 0))
        THEN `2_new_org_name_alteryx_lag1`
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lag1`) = 0))
        THEN `2_new_org_name_alteryx_lead1`
      ELSE `2_new_org_name_alteryx`
    END AS `2_new_org_name_alteryx`,
    * EXCEPT (`2_new_org_name_alteryx_lead1`, `2_new_org_name_alteryx_lag1`, `2_new_org_name_alteryx`)
  
  FROM MultiRowFormula_924_window AS in0

),

MultiRowFormula_925_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM MultiRowFormula_924_0 AS in0

),

MultiRowFormula_925_0 AS (

  SELECT 
    lead(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lead1`,
    lag(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lag1`,
    *
  
  FROM MultiRowFormula_925_row_id_0 AS in0

),

MultiRowFormula_925_1 AS (

  SELECT 
    CASE
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lead1`) = 0))
        THEN `1_new_org_name_alteryx_lag1`
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lag1`) = 0))
        THEN `1_new_org_name_alteryx_lead1`
      ELSE `1_new_org_name_alteryx`
    END AS `1_new_org_name_alteryx`,
    * EXCEPT (`1_new_org_name_alteryx_lead1`, `1_new_org_name_alteryx_lag1`, `1_new_org_name_alteryx`)
  
  FROM MultiRowFormula_925_0 AS in0

),

MultiRowFormula_925_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_925_1 AS in0

),

Formula_922_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(`2_new_org_name_alteryx`) AS BOOLEAN)
        THEN `1_new_org_name_alteryx`
      ELSE `2_new_org_name_alteryx`
    END AS STRING) AS new_org_name_alteryx,
    *
  
  FROM MultiRowFormula_925_row_id_drop_0 AS in0

),

Union_914 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_913_inner', 'Formula_922_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}]', 
        '[{"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_892 AS (

  SELECT 
    first(new_org_name_alteryx) AS new_org_name_alteryx,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Union_914 AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_888 AS (

  SELECT * 
  
  FROM Summarize_883 AS in0
  
  WHERE (Max_Count = CAST('1' AS DOUBLE))

),

Join_884_inner AS (

  SELECT 
    in1.new_organization_name_fuzzy_unique AS new_org_name_alteryx,
    in1.Count AS `Count`,
    in1.GroupID AS GroupID
  
  FROM Filter_888 AS in0
  INNER JOIN Summarize_878 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.Max_Count = in1.Count))

),

Summarize_902 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Join_884_inner AS in0
  
  GROUP BY GroupID

),

Filter_903_reject AS (

  SELECT * 
  
  FROM Summarize_902 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Join_898_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_903_reject AS in0
  INNER JOIN Join_884_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_903 AS (

  SELECT * 
  
  FROM Summarize_902 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Join_901_inner AS (

  SELECT 
    in0.GroupID AS GroupID,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.Count AS `Count`
  
  FROM Filter_903 AS in0
  INNER JOIN Join_884_inner AS in1
     ON (in0.GroupID = in1.GroupID)

),

RegEx_911 AS (

  {{
    prophecy_basics.Regex(
      ['Join_901_inner'], 
      [
        {
          'columnName': 'regex_col1', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[A-Z_0-9](\\s|:|,|-|l{1}|\\+|/|\\.).*$)'
        }, 
        {
          'columnName': 'regex_col2', 
          'dataType': 'string', 
          'rgxExpression': '(^(\\\\u{1})(\\s|-|\\.|\\+|,).*$)'
        }, 
        {
          'columnName': 'regex_col3', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\d).(-|\\s|.|,|/+|(?:^\\w+)).*$)'
        }, 
        { 'columnName': 'regex_col4', 'dataType': 'string', 'rgxExpression': '((?:^\\w+)$)' }, 
        {
          'columnName': 'regex_col5', 
          'dataType': 'string', 
          'rgxExpression': '((?:^\\w+)[a-z_0-9](:|,|-|\\+|/).*$)'
        }
      ], 
      '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}]', 
      'new_org_name_alteryx', 
      '((?:^\w+)[A-Z_0-9](\s|:|,|-|l{1}|\+|/|\.).*$)|(^(\\u{1})(\s|-|\.|\+|,).*$)|((?:^\d).(-|\s|.|,|/+|(?:^\w+)).*$)|((?:^\w+)$)|((?:^\w+)[a-z_0-9](:|,|-|\+|/).*$)', 
      'match', 
      false, 
      false, 
      '', 
      false, 
      'splitColumns', 
      1, 
      'dropExtraWithoutWarning', 
      '', 
      'starts_with_uppercase_letter', 
      false, 
      '_replaced'
    )
  }}

),

RegEx_911_typeCastGem AS (

  SELECT 
    CAST(starts_with_uppercase_letter AS BOOLEAN) AS starts_with_uppercase_letter,
    * EXCEPT (`starts_with_uppercase_letter`)
  
  FROM RegEx_911 AS in0

),

Filter_900_reject AS (

  SELECT * 
  
  FROM RegEx_911_typeCastGem AS in0
  
  WHERE (
          NOT (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (
                                                                                          (
                                                                                            (length(new_org_name_alteryx) < 3)
                                                                                            OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                          )
                                                                                          OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                        )
                                                                                        OR (starts_with_uppercase_letter = true)
                                                                                      )
                                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                    )
                                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                          )
                                                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                        )
                                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                      )
                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
            )
          )
          OR isnull(
               (
                 (
                   (
                     (
                       (
                         (
                           (
                             (
                               (
                                 (
                                   (
                                     (
                                       (
                                         (
                                           (
                                             (
                                               (
                                                 (
                                                   (
                                                     (
                                                       (
                                                         (
                                                           (
                                                             (
                                                               (
                                                                 (
                                                                   (
                                                                     (
                                                                       (
                                                                         (
                                                                           (
                                                                             (
                                                                               (
                                                                                 (
                                                                                   (
                                                                                     (
                                                                                       (
                                                                                         (
                                                                                           (
                                                                                             (
                                                                                               (length(new_org_name_alteryx) < 3)
                                                                                               OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                             )
                                                                                             OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                           )
                                                                                           OR (starts_with_uppercase_letter = true)
                                                                                         )
                                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                       )
                                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                                     )
                                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                                   )
                                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                                 )
                                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                               )
                                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                             )
                                                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                           )
                                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                         )
                                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                       )
                                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                                     )
                                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                                   )
                                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                                 )
                                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                               )
                                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                             )
                                                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                           )
                                                           OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                         )
                                                         OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                       )
                                                       OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                                     )
                                                     OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                                   )
                                                   OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                                 )
                                                 OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                               )
                                               OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                             )
                                             OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                           )
                                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                         )
                                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                       )
                                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                                     )
                                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                                   )
                                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                                 )
                                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                               )
                               OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                             )
                             OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                           )
                           OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                         )
                         OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                       )
                       OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
                     )
                     OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
                   )
                   OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
                 )
                 OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
               ))
        )

),

Summarize_904 AS (

  SELECT 
    first(new_org_name_alteryx) AS `2_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_900_reject AS in0
  
  GROUP BY 
    Count, GroupID

),

Filter_900 AS (

  SELECT * 
  
  FROM RegEx_911_typeCastGem AS in0
  
  WHERE (
          (
            (
              (
                (
                  (
                    (
                      (
                        (
                          (
                            (
                              (
                                (
                                  (
                                    (
                                      (
                                        (
                                          (
                                            (
                                              (
                                                (
                                                  (
                                                    (
                                                      (
                                                        (
                                                          (
                                                            (
                                                              (
                                                                (
                                                                  (
                                                                    (
                                                                      (
                                                                        (
                                                                          (
                                                                            (
                                                                              (
                                                                                (
                                                                                  (
                                                                                    (
                                                                                      (
                                                                                        (length(new_org_name_alteryx) < 3)
                                                                                        OR coalesce(contains(lower(new_org_name_alteryx), lower('?')), false)
                                                                                      )
                                                                                      OR (substring(new_org_name_alteryx, 1, 1) = '(')
                                                                                    )
                                                                                    OR (starts_with_uppercase_letter = true)
                                                                                  )
                                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STE ')), false)
                                                                                )
                                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE.')), false)
                                                                              )
                                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD ')), false)
                                                                            )
                                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RUE ')), false)
                                                                          )
                                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' BLVD.')), false)
                                                                        )
                                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower('BOULEVARD')), false)
                                                                      )
                                                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVENUE')), false)
                                                                    )
                                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST.')), false)
                                                                  )
                                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ST,')), false)
                                                                )
                                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' STR.')), false)
                                                              )
                                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' UL.')), false)
                                                            )
                                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD,')), false)
                                                          )
                                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' LANE')), false)
                                                        )
                                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                      )
                                                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = ' W')
                                                    )
                                                    OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PLAZA')), false)
                                                  )
                                                  OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' ASSOCIATES')), false)
                                                )
                                                OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARK')), false)
                                              )
                                              OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' RD.')), false)
                                            )
                                            OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' QUAY')), false)
                                          )
                                          OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' SQUARE DU')), false)
                                        )
                                        OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' AVE ')), false)
                                      )
                                      OR coalesce(contains(lower(upper(new_org_name_alteryx)), lower(' PARKWAY')), false)
                                    )
                                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'STREET')
                                  )
                                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'ROAD')
                                )
                                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 6) + 1), 6) = 'AVENUE')
                              )
                              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'BLV')
                            )
                            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = 'BLVD')
                          )
                          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'PLAZA')
                        )
                        OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 5) + 1), 5) = 'DRIVE')
                      )
                      OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 4) + 1), 4) = ' WAY')
                    )
                    OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 8) + 1), 8) = ' HIGHWAY')
                  )
                  OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'AVE')
                )
                OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = 'STR')
              )
              OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 2) + 1), 2) = 'DR')
            )
            OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' ST')
          )
          OR (substring(CAST(upper(new_org_name_alteryx) AS STRING), ((length(new_org_name_alteryx) - 3) + 1), 3) = ' RD')
        )

),

Summarize_905 AS (

  SELECT 
    first(new_org_name_alteryx) AS `1_new_org_name_alteryx`,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Filter_900 AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_906 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_904', 'Summarize_905'], 
      [
        '[{"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]', 
        '[{"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

MultiRowFormula_909_window AS (

  SELECT 
    *,
    lead(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lead1`,
    lag(`2_new_org_name_alteryx`, 1) OVER (PARTITION BY GroupID ORDER BY GroupID ASC NULLS FIRST) AS `2_new_org_name_alteryx_lag1`
  
  FROM Union_906 AS in0

),

MultiRowFormula_909_0 AS (

  SELECT 
    CASE
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lead1`) = 0))
        THEN `2_new_org_name_alteryx_lag1`
      WHEN ((length(`2_new_org_name_alteryx`) = 0) AND (length(`2_new_org_name_alteryx_lag1`) = 0))
        THEN `2_new_org_name_alteryx_lead1`
      ELSE `2_new_org_name_alteryx`
    END AS `2_new_org_name_alteryx`,
    * EXCEPT (`2_new_org_name_alteryx_lead1`, `2_new_org_name_alteryx_lag1`, `2_new_org_name_alteryx`)
  
  FROM MultiRowFormula_909_window AS in0

),

MultiRowFormula_910_row_id_0 AS (

  SELECT 
    (monotonically_increasing_id()) AS prophecy_row_id,
    *
  
  FROM MultiRowFormula_909_0 AS in0

),

MultiRowFormula_910_0 AS (

  SELECT 
    lead(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lead1`,
    lag(`1_new_org_name_alteryx`, 1) OVER (PARTITION BY 1 ORDER BY prophecy_row_id ASC NULLS FIRST) AS `1_new_org_name_alteryx_lag1`,
    *
  
  FROM MultiRowFormula_910_row_id_0 AS in0

),

MultiRowFormula_910_1 AS (

  SELECT 
    CASE
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lead1`) = 0))
        THEN `1_new_org_name_alteryx_lag1`
      WHEN ((length(`1_new_org_name_alteryx`) = 0) AND (length(`1_new_org_name_alteryx_lag1`) = 0))
        THEN `1_new_org_name_alteryx_lead1`
      ELSE `1_new_org_name_alteryx`
    END AS `1_new_org_name_alteryx`,
    * EXCEPT (`1_new_org_name_alteryx_lead1`, `1_new_org_name_alteryx_lag1`, `1_new_org_name_alteryx`)
  
  FROM MultiRowFormula_910_0 AS in0

),

MultiRowFormula_910_row_id_drop_0 AS (

  SELECT * EXCEPT (`prophecy_row_id`)
  
  FROM MultiRowFormula_910_1 AS in0

),

Formula_907_0 AS (

  SELECT 
    CAST(CASE
      WHEN CAST(isnull(`2_new_org_name_alteryx`) AS BOOLEAN)
        THEN `1_new_org_name_alteryx`
      ELSE `2_new_org_name_alteryx`
    END AS STRING) AS new_org_name_alteryx,
    *
  
  FROM MultiRowFormula_910_row_id_drop_0 AS in0

),

Union_899 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_898_inner', 'Formula_907_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}]', 
        '[{"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "1_new_org_name_alteryx", "dataType": "String"}, {"name": "2_new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_889 AS (

  SELECT 
    first(new_org_name_alteryx) AS new_org_name_alteryx,
    Count AS `Count`,
    GroupID AS GroupID
  
  FROM Union_899 AS in0
  
  GROUP BY 
    Count, GroupID

),

Union_891 AS (

  {{
    prophecy_basics.UnionByName(
      ['Summarize_889', 'Summarize_892'], 
      [
        '[{"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]', 
        '[{"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "Count", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Join_880_inner AS (

  SELECT 
    in1.latitude AS latitude,
    in1.organization_name AS organization_name,
    in1.longitude AS longitude,
    in1.city_town_village AS city_town_village,
    in1.lot AS lot,
    in0.new_org_name_alteryx AS new_org_name_alteryx,
    in1.city_town_village_old AS city_town_village_old,
    in1.lat AS lat,
    in1.GroupID AS GroupID
  
  FROM Union_891 AS in0
  INNER JOIN Formula_961_0 AS in1
     ON ((in0.GroupID = in1.GroupID) AND (in0.new_org_name_alteryx = in1.new_organization_name_fuzzy_unique))

),

Join_880_right AS (

  SELECT in0.*
  
  FROM Formula_961_0 AS in0
  ANTI JOIN Union_891 AS in1
     ON ((in1.GroupID = in0.GroupID) AND (in1.new_org_name_alteryx = in0.new_organization_name_fuzzy_unique))

),

AlteryxSelect_882 AS (

  SELECT 
    lat AS lat,
    lot AS lot,
    GroupID AS GroupID,
    city_town_village_old AS city_town_village_old,
    latitude AS latitude,
    longitude AS longitude,
    organization_name AS organization_name,
    city_town_village AS city_town_village,
    new_organization_name_fuzzy_unique AS new_org_name_alteryx,
    CAST(NULL AS string) AS organization_id,
    CAST(NULL AS string) AS new_organization_name
  
  FROM Join_880_right AS in0

),

Union_881 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_880_inner', 'AlteryxSelect_882'], 
      [
        '[{"name": "latitude", "dataType": "Double"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}]', 
        '[{"name": "lat", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "GroupID", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "organization_name", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "organization_id", "dataType": "String"}, {"name": "new_organization_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Summarize_893 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Union_881 AS in0
  
  GROUP BY GroupID

),

Filter_894 AS (

  SELECT * 
  
  FROM Summarize_893 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 0)

),

Join_895_inner AS (

  SELECT 
    in0.*,
    in1.* EXCEPT (`GroupID`)
  
  FROM Filter_894 AS in0
  INNER JOIN Union_881 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Filter_894_reject AS (

  SELECT * 
  
  FROM Summarize_893 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 0)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 0) IS NULL)
        )

),

Join_896_inner AS (

  SELECT 
    in1.latitude AS latitude,
    in1.new_organization_name AS new_organization_name,
    in1.organization_name AS organization_name,
    in1.longitude AS longitude,
    in0.CountDistinctNonNull_new_org_name_alteryx AS CountDistinctNonNull_new_org_name_alteryx,
    in1.city_town_village AS city_town_village,
    in1.organization_id AS organization_id,
    in1.lot AS lot,
    in1.lat AS lat
  
  FROM Filter_894_reject AS in0
  INNER JOIN Union_881 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Union_897 AS (

  {{
    prophecy_basics.UnionByName(
      ['Join_895_inner', 'Join_896_inner'], 
      [
        '[{"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Bigint"}, {"name": "GroupID", "dataType": "String"}, {"name": "latitude", "dataType": "Double"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "city_town_village", "dataType": "String"}, {"name": "lot", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "organization_id", "dataType": "String"}, {"name": "new_organization_name", "dataType": "String"}]', 
        '[{"name": "latitude", "dataType": "Double"}, {"name": "new_organization_name", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}, {"name": "longitude", "dataType": "Double"}, {"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Bigint"}, {"name": "city_town_village", "dataType": "String"}, {"name": "organization_id", "dataType": "String"}, {"name": "lot", "dataType": "Double"}, {"name": "lat", "dataType": "Double"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_1021 AS (

  SELECT * 
  
  FROM Union_897 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, new_org_name_alteryx ORDER BY GroupID, new_org_name_alteryx) = 1

),

Summarize_1020 AS (

  SELECT 
    COUNT(DISTINCT new_org_name_alteryx) AS CountDistinctNonNull_new_org_name_alteryx,
    GroupID AS GroupID
  
  FROM Unique_1021 AS in0
  
  GROUP BY GroupID

),

Filter_1022 AS (

  SELECT * 
  
  FROM Summarize_1020 AS in0
  
  WHERE (CountDistinctNonNull_new_org_name_alteryx > 1)

),

Unique_1018 AS (

  SELECT * 
  
  FROM Filter_1022 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, CountDistinctNonNull_new_org_name_alteryx ORDER BY GroupID, CountDistinctNonNull_new_org_name_alteryx) = 1

),

Macro_1070 AS (

  {{
    prophecy_basics.ToDo(
      'Failed to parse Macro. Please upload the file /sdcuns600vfs02/QPM_Analytics/Site_Master_Wuhan//keyword match.yxmc to resolve it.'
    )
  }}

),

AlteryxSelect_1077 AS (

  SELECT 
    new_org_name_alteryx AS new_org_name_alteryx,
    org_name_keyword AS org_name_keyword,
    * EXCEPT (`new_org_name_alteryx`, `org_name_keyword`)
  
  FROM Macro_1070 AS in0

),

Union_1024_reformat_0 AS (

  SELECT 
    CAST(CountDistinctNonNull_new_org_name_alteryx AS INTEGER) AS CountDistinctNonNull_new_org_name_alteryx,
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    CAST(lat AS DOUBLE) AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(lot AS DOUBLE) AS lot,
    new_org_name_alteryx AS new_org_name_alteryx,
    CAST(org_name_keyword AS string) AS org_name_keyword,
    organization_name AS organization_name
  
  FROM AlteryxSelect_1077 AS in0

),

Filter_1022_reject AS (

  SELECT * 
  
  FROM Summarize_1020 AS in0
  
  WHERE (
          (
            NOT(
              CountDistinctNonNull_new_org_name_alteryx > 1)
          )
          OR ((CountDistinctNonNull_new_org_name_alteryx > 1) IS NULL)
        )

),

Unique_1025 AS (

  SELECT * 
  
  FROM Filter_1022_reject AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY GroupID, CountDistinctNonNull_new_org_name_alteryx ORDER BY GroupID, CountDistinctNonNull_new_org_name_alteryx) = 1

),

Join_1023_inner AS (

  SELECT 
    in0.* EXCEPT (`GroupID`, `CountDistinctNonNull_new_org_name_alteryx`),
    in1.* EXCEPT (`CountDistinctNonNull_new_org_name_alteryx`)
  
  FROM Unique_1025 AS in0
  INNER JOIN Union_897 AS in1
     ON (in0.GroupID = in1.GroupID)

),

Union_1024_reformat_1 AS (

  SELECT 
    CAST(GroupID AS string) AS GroupID,
    city_town_village AS city_town_village,
    city_town_village_old AS city_town_village_old,
    CAST(lat AS DOUBLE) AS lat,
    CAST(latitude AS DOUBLE) AS latitude,
    CAST(longitude AS DOUBLE) AS longitude,
    CAST(lot AS DOUBLE) AS lot,
    new_org_name_alteryx AS new_org_name_alteryx,
    CAST(new_organization_name AS string) AS new_organization_name,
    CAST(organization_id AS string) AS organization_id,
    organization_name AS organization_name
  
  FROM Join_1023_inner AS in0

),

Union_1024 AS (

  {{
    prophecy_basics.UnionByName(
      ['Union_1024_reformat_1', 'Union_1024_reformat_0'], 
      [
        '[{"name": "GroupID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "new_organization_name", "dataType": "String"}, {"name": "organization_id", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}]', 
        '[{"name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer"}, {"name": "GroupID", "dataType": "String"}, {"name": "city_town_village", "dataType": "String"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "lat", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "lot", "dataType": "Double"}, {"name": "new_org_name_alteryx", "dataType": "String"}, {"name": "org_name_keyword", "dataType": "String"}, {"name": "organization_name", "dataType": "String"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Cleanse_933 AS (

  {{
    prophecy_basics.DataCleansing(
      ['Union_1024'], 
      [
        { "name": "GroupID", "dataType": "String" }, 
        { "name": "city_town_village", "dataType": "String" }, 
        { "name": "city_town_village_old", "dataType": "String" }, 
        { "name": "lat", "dataType": "Double" }, 
        { "name": "latitude", "dataType": "Double" }, 
        { "name": "longitude", "dataType": "Double" }, 
        { "name": "lot", "dataType": "Double" }, 
        { "name": "new_org_name_alteryx", "dataType": "String" }, 
        { "name": "new_organization_name", "dataType": "String" }, 
        { "name": "organization_id", "dataType": "String" }, 
        { "name": "organization_name", "dataType": "String" }, 
        { "name": "CountDistinctNonNull_new_org_name_alteryx", "dataType": "Integer" }, 
        { "name": "org_name_keyword", "dataType": "String" }
      ], 
      'keepOriginal', 
      ['new_org_name_alteryx'], 
      false, 
      '', 
      false, 
      0, 
      true, 
      true, 
      false, 
      false, 
      false, 
      false, 
      false, 
      false, 
      '1970-01-01', 
      false, 
      '1970-01-01 00:00:00.0'
    )
  }}

),

Summarize_934 AS (

  SELECT 
    COUNT(DISTINCT city_town_village) AS CountDistinctNonNull_city_town_village,
    new_org_name_alteryx AS new_org_name_alteryx
  
  FROM Cleanse_933 AS in0
  
  GROUP BY new_org_name_alteryx

),

Filter_936 AS (

  SELECT * 
  
  FROM Summarize_934 AS in0
  
  WHERE (CountDistinctNonNull_city_town_village > 1)

),

Join_935_right_UnionRightOuter AS (

  SELECT 
    in1.city_town_village AS city_town_village,
    in1.lat AS lat,
    in1.lot AS lot,
    in1.latitude AS latitude,
    in1.longitude AS longitude,
    in1.new_org_name_alteryx AS new_org_name_alteryx,
    in1.organization_name AS organization_name,
    in0.CountDistinctNonNull_city_town_village AS CountDistinctNonNull_city_town_village,
    in1.CountDistinctNonNull_new_org_name_alteryx AS CountDistinctNonNull_new_org_name_alteryx,
    in1.GroupID AS GroupID,
    in1.city_town_village_old AS city_town_village_old,
    in1.org_name_keyword AS org_name_keyword,
    in0.* EXCEPT (`new_org_name_alteryx`, `CountDistinctNonNull_city_town_village`),
    in1.* EXCEPT (`city_town_village`, 
    `lat`, 
    `lot`, 
    `latitude`, 
    `longitude`, 
    `new_org_name_alteryx`, 
    `organization_name`, 
    `CountDistinctNonNull_new_org_name_alteryx`, 
    `GroupID`, 
    `city_town_village_old`, 
    `org_name_keyword`)
  
  FROM Filter_936 AS in0
  RIGHT JOIN Cleanse_933 AS in1
     ON (in0.new_org_name_alteryx = in1.new_org_name_alteryx)

),

Formula_938_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (
          ((CountDistinctNonNull_city_town_village > 1) AND ((LENGTH(new_org_name_alteryx)) > 0))
          AND ((LENGTH(city_town_village)) > 0)
        )
          THEN CAST((CONCAT(new_org_name_alteryx, ' - ', city_town_village)) AS string)
        WHEN ((CountDistinctNonNull_city_town_village > 1) AND ((LENGTH(new_org_name_alteryx)) = 0))
          THEN CAST(NULL AS string)
        ELSE new_org_name_alteryx
      END
    ) AS string) AS new_org_name_alteryx,
    * EXCEPT (`new_org_name_alteryx`)
  
  FROM Join_935_right_UnionRightOuter AS in0

),

AlteryxSelect_939 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS lat,
    lot AS lot,
    latitude AS latitude,
    longitude AS longitude,
    new_org_name_alteryx AS new_org_name_alteryx,
    organization_name AS organization_name,
    GroupID AS GroupID,
    city_town_village_old AS city_town_village_old,
    org_name_keyword AS org_name_keyword
  
  FROM Formula_938_0 AS in0

),

AlteryxSelect_778 AS (

  SELECT 
    lat AS lat,
    lot AS lot
  
  FROM AlteryxSelect_939 AS in0

),

Unique_783 AS (

  SELECT * 
  
  FROM AlteryxSelect_778 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY lat, lot ORDER BY lat, lot) = 1

),

RecordID_776 AS (

  {{
    prophecy_basics.RecordID(
      ['Unique_783'], 
      'incremental_id', 
      'RecordID', 
      'integer', 
      6, 
      1, 
      'tableLevel', 
      'first_column', 
      [], 
      []
    )
  }}

),

Join_779_inner AS (

  SELECT 
    in0.RecordID AS GroupID_Geo,
    in1.GroupID AS GroupID_Distance,
    in0.* EXCEPT (`RecordID`, `lat`, `lot`),
    in1.* EXCEPT (`GroupID`)
  
  FROM RecordID_776 AS in0
  INNER JOIN AlteryxSelect_939 AS in1
     ON ((in0.lat = in1.lat) AND (in0.lot = in1.lot))

),

AlteryxSelect_407 AS (

  SELECT 
    city_town_village AS city_town_village,
    lat AS latitude1,
    lot AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    organization_name AS org_name,
    new_org_name_alteryx AS new_org_name,
    GroupID_Distance AS GroupID_Distance,
    GroupID_Geo AS GroupID_Geo,
    city_town_village_old AS city_town_village_old,
    org_name_keyword AS org_name_keyword
  
  FROM Join_779_inner AS in0

),

Filter_240 AS (

  SELECT * 
  
  FROM Summarize_239 AS in0
  
  WHERE ((Count_lat = 1) AND (Count_long = 1))

),

Join_247_inner AS (

  SELECT 
    in1.latitude1 AS Right_latitude1,
    in1.longitude1 AS Right_longitude1,
    in0.*,
    in1.* EXCEPT (`latitude1`, `longitude1`)
  
  FROM Filter_221 AS in0
  INNER JOIN Filter_240 AS in1
     ON ((in0.latitude1 = in1.latitude1) AND (in0.longitude1 = in1.longitude1))

),

Formula_1006_0 AS (

  SELECT 
    CAST(city_town_village AS string) AS city_town_village_old,
    *
  
  FROM Join_247_inner AS in0

),

Union_405 AS (

  {{
    prophecy_basics.UnionByName(
      ['AlteryxSelect_407', 'Formula_1006_0'], 
      [
        '[{"name": "city_town_village", "dataType": "String"}, {"name": "latitude1", "dataType": "Double"}, {"name": "longitude1", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}, {"name": "new_org_name", "dataType": "String"}, {"name": "GroupID_Distance", "dataType": "String"}, {"name": "GroupID_Geo", "dataType": "Integer"}, {"name": "city_town_village_old", "dataType": "String"}, {"name": "org_name_keyword", "dataType": "String"}]', 
        '[{"name": "city_town_village_old", "dataType": "String"}, {"name": "Right_latitude1", "dataType": "Double"}, {"name": "Right_longitude1", "dataType": "Double"}, {"name": "contact_id", "dataType": "Decimal"}, {"name": "city_town_village", "dataType": "String"}, {"name": "latitude1", "dataType": "Double"}, {"name": "longitude1", "dataType": "Double"}, {"name": "latitude", "dataType": "Double"}, {"name": "longitude", "dataType": "Double"}, {"name": "org_name", "dataType": "String"}, {"name": "Count_lat", "dataType": "Bigint"}, {"name": "Count_long", "dataType": "Bigint"}]'
      ], 
      'allowMissingColumns'
    )
  }}

),

Unique_419 AS (

  SELECT * 
  
  FROM Union_405 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY city_town_village, 
  latitude1, 
  longitude1, 
  latitude, 
  longitude, 
  org_name, 
  city_town_village_old, 
  new_org_name, 
  GroupID_Distance, 
  GroupID_Geo ORDER BY city_town_village, latitude1, longitude1, latitude, longitude, org_name, city_town_village_old, new_org_name, GroupID_Distance, GroupID_Geo) = 1

),

aka_GPDIP_EDLUD_412 AS (

  SELECT *
  
  FROM {{ prophecy_tmp_source('facility_master_wf_1_', 'aka_GPDIP_EDLUD_412') }}

),

AlteryxSelect_413 AS (

  SELECT 
    contact_id AS contact_id,
    person_id AS person_id,
    organization_id AS organization_id,
    address_id AS address_id,
    contact_role AS contact_role,
    contact_status AS contact_status,
    primary_contact AS primary_contact,
    study_id AS study_id,
    study_site_number AS study_site_number,
    country_name AS country_name,
    mail_box_suite AS mail_box_suite,
    city_town_village AS city_town_village,
    postal_cd AS postal_cd,
    state_province_county AS state_province_county,
    address_line1_cleansed AS address_line1_cleansed,
    address_line2_cleansed AS address_line2_cleansed,
    geoaccuracy AS geoaccuracy,
    geocodingsystem AS geocodingsystem,
    street AS street,
    street_continued AS street_continued,
    street_cleansed AS street_cleansed,
    zip4 AS zip4,
    zip5 AS zip5,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    fips AS fips,
    county AS county,
    state2 AS state2,
    state AS state,
    org_name AS org_name,
    person_full_name AS person_full_name,
    load_date AS load_date
  
  FROM aka_GPDIP_EDLUD_412 AS in0

),

Join_414_left_UnionLeftOuter AS (

  SELECT 
    in0.state2 AS state2,
    in0.postal_cd AS postal_cd,
    in0.mail_box_suite AS mail_box_suite,
    in0.load_date AS load_date,
    in0.latitude AS latitude,
    in0.zip5 AS zip5,
    in0.state AS state,
    in0.address_line1_cleansed AS address_line1_cleansed,
    in0.study_id AS study_id,
    in0.street_continued AS street_continued,
    in0.person_id AS person_id,
    in0.primary_contact AS primary_contact,
    in1.GroupID_Distance AS GroupID_Distance,
    in0.address_id AS address_id,
    in0.fips AS fips,
    in0.zip4 AS zip4,
    in0.geoaccuracy AS geoaccuracy,
    in0.geocodingsystem AS geocodingsystem,
    in0.longitude AS longitude,
    in1.org_name_keyword AS org_name_keyword,
    in0.county AS county,
    in0.person_full_name AS person_full_name,
    in0.city_town_village AS city_town_village,
    in0.org_name AS org_name,
    in0.contact_status AS contact_status,
    in0.organization_id AS organization_id,
    in0.longitude1 AS longitude1,
    in0.street AS street,
    in0.state_province_county AS state_province_county,
    in0.contact_id AS contact_id,
    in0.street_cleansed AS street_cleansed,
    in0.latitude1 AS latitude1,
    in0.country_name AS country_name,
    in0.contact_role AS contact_role,
    in1.new_org_name AS new_org_name,
    in0.study_site_number AS study_site_number,
    in0.address_line2_cleansed AS address_line2_cleansed,
    in1.GroupID_Geo AS GroupID_Geo,
    (
      CASE
        WHEN (
          (
            ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))
            AND (in0.org_name = in1.org_name)
          )
          AND (in0.city_town_village = in1.city_town_village_old)
        )
          THEN in1.city_town_village
        ELSE NULL
      END
    ) AS city_town_village_cleansed
  
  FROM AlteryxSelect_413 AS in0
  LEFT JOIN Unique_419 AS in1
     ON (
      (
        ((in0.latitude = in1.latitude) AND (in0.longitude = in1.longitude))
        AND (in0.org_name = in1.org_name)
      )
      AND (in0.city_town_village = in1.city_town_village_old)
    )

),

Formula_522_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN (NOT(new_org_name IS NULL))
          THEN new_org_name
        ELSE org_name
      END
    ) AS string) AS org_name_adj,
    *
  
  FROM Join_414_left_UnionLeftOuter AS in0

),

Unique_424 AS (

  SELECT * 
  
  FROM Formula_522_0 AS in0
  
  QUALIFY ROW_NUMBER() OVER (PARTITION BY contact_id, 
  person_id, 
  organization_id, 
  address_id, 
  contact_role, 
  contact_status, 
  primary_contact, 
  study_id, 
  study_site_number, 
  country_name, 
  mail_box_suite, 
  postal_cd, 
  state_province_county, 
  address_line1_cleansed, 
  address_line2_cleansed, 
  geoaccuracy, 
  geocodingsystem, 
  street, 
  street_continued, 
  street_cleansed, 
  zip4, 
  zip5, 
  fips, 
  county, 
  state2, 
  state, 
  person_full_name, 
  city_town_village, 
  city_town_village_cleansed, 
  latitude1, 
  longitude1, 
  latitude, 
  longitude, 
  org_name, 
  load_date, 
  GroupID_Distance, 
  GroupID_Geo, 
  new_org_name, 
  org_name_keyword, 
  org_name_adj ORDER BY contact_id, person_id, organization_id, address_id, contact_role, contact_status, primary_contact, study_id, study_site_number, country_name, mail_box_suite, postal_cd, state_province_county, address_line1_cleansed, address_line2_cleansed, geoaccuracy, geocodingsystem, street, street_continued, street_cleansed, zip4, zip5, fips, county, state2, state, person_full_name, city_town_village, city_town_village_cleansed, latitude1, longitude1, latitude, longitude, org_name, load_date, GroupID_Distance, GroupID_Geo, new_org_name, org_name_keyword, org_name_adj) = 1

),

AlteryxSelect_525 AS (

  SELECT 
    contact_id AS contact_id,
    person_id AS person_id,
    organization_id AS organization_id,
    address_id AS address_id,
    contact_role AS contact_role,
    contact_status AS contact_status,
    primary_contact AS primary_contact,
    study_id AS study_id,
    study_site_number AS study_site_number,
    country_name AS country_name,
    mail_box_suite AS mail_box_suite,
    postal_cd AS postal_cd,
    state_province_county AS state_province_county,
    address_line1_cleansed AS address_line1_cleansed,
    address_line2_cleansed AS address_line2_cleansed,
    geoaccuracy AS geoaccuracy,
    geocodingsystem AS geocodingsystem,
    street AS street,
    street_continued AS street_continued,
    street_cleansed AS street_cleansed,
    zip4 AS zip4,
    zip5 AS zip5,
    fips AS fips,
    county AS county,
    state2 AS state2,
    state AS state,
    person_full_name AS person_full_name,
    city_town_village AS city_town_village,
    city_town_village_cleansed AS city_town_village_cleansed,
    latitude1 AS latitude1,
    longitude1 AS longitude1,
    latitude AS latitude,
    longitude AS longitude,
    org_name AS org_name,
    org_name_adj AS org_name_adj,
    org_name_keyword AS org_name_keyword,
    CAST(GroupID_Distance AS string) AS GroupID_Distance,
    CAST(GroupID_Geo AS string) AS GroupID_Geo,
    load_date AS load_date
  
  FROM Unique_424 AS in0

),

Formula_793_to_Formula_1078_0 AS (

  SELECT 
    CAST((
      CASE
        WHEN CAST((GroupID_Distance IS NULL) AS BOOLEAN)
          THEN NULL
        ELSE (
          CONCAT(
            'Distance-', 
            (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID_Distance AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
        )
      END
    ) AS string) AS GroupID_Distance,
    CAST((
      CASE
        WHEN CAST((GroupID_Geo IS NULL) AS BOOLEAN)
          THEN NULL
        ELSE (
          CONCAT(
            'Geo-', 
            (REGEXP_REPLACE((REGEXP_REPLACE((FORMAT_NUMBER(CAST(GroupID_Geo AS DOUBLE), 0)), ',', '__THS__')), '__THS__', '')))
        )
      END
    ) AS string) AS GroupID_Geo,
    CAST(NULL AS string) AS variableNull,
    * EXCEPT (`groupid_distance`, `groupid_geo`)
  
  FROM AlteryxSelect_525 AS in0

)

SELECT *

FROM Formula_793_to_Formula_1078_0
