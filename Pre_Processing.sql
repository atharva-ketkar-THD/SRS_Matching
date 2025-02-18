------------- Lanes with atleast 20 empties since Jan 2024
CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND` AS    
(
  SELECT flt.CAMPUS AS REGION,
        flt.CAMPUS_NBR_NM,
        B.DC_NBR, 
        flt.SCAC,
        A.ORIG_FAC_ALS_ID AS O_ID,
        A.ORIG_FAC_TYP_IND AS O_TYPE, 
        A.ORIG_PSTL_CD AS O_ZIP,
        o1.CITY_NM AS O_CITY,
        o1.ST_CD AS O_STATE,
        COALESCE(CAST(new_bdc.NEW_BDC_NBR AS STRING), A.DEST_FAC_ALS_ID) AS D_ID,
        A.DEST_FAC_TYP_IND AS D_TYPE,
        A.DEST_PSTL_CD AS D_ZIP,
        d1.CITY_NM AS D_CITY,
        d1.ST_CD AS D_STATE,
        'SO' AS LEG,
        AVG(A.MILE_CNT) AS DIST,
        SUM(1) AS LOADS,
        10000 AS OW_COST_PER_LOAD
  FROM `pr-edw-views-thd.SCHN_TRANS.CARR_INVC_UPLD` A  
  LEFT JOIN `analytics-supplychain-thd.KIRAN.BDC_TRANSITIONS` new_bdc 
    ON CAST(A.DEST_FAC_ALS_ID AS STRING) = CAST(new_bdc.OLD_BDC_NBR AS STRING)
  LEFT JOIN `pr-edw-views-thd.SCHN_TRANS.CARR_INVC` B    
    ON A.THD_CARR_INVC_ID = B.THD_CARR_INVC_ID
  LEFT JOIN `pr-edw-views-thd.SHARED.CAL_PRD_HIER_FD` CAL           
    ON COALESCE(SHP_DT,
                CAST(ARVL_TS AS DATE),
                CAST(DPRT_TS AS DATE)) = CAL.CAL_DT
  LEFT JOIN `analytics-supplychain-thd.LAB_TRANS_ANALYTICS.FLATBED_CAMPUS_MAPPING` flt  
    ON B.DC_NBR = flt.CAMPUS_NAME
  LEFT JOIN 
  (
    SELECT DISTINCT LOC_NBR, CITY_NM, ST_CD, PSTL_CD
    FROM `pr-edw-views-thd.SCHN_CURATED.SCHN_LOC_ATTR`
  ) o1  
    ON A.ORIG_FAC_ALS_ID = o1.LOC_NBR
  LEFT JOIN
  (
    SELECT DISTINCT LOC_NBR, CITY_NM, ST_CD, PSTL_CD
    FROM `pr-edw-views-thd.SCHN_CURATED.SCHN_LOC_ATTR`
  ) d1  
    ON A.DEST_FAC_ALS_ID = d1.LOC_NBR
  WHERE 1=1
        AND CAL.CAL_DT >= '2024-01-01'
        AND OND_SHP_TYP_CD IN (25)
        AND CONCAT(ORIG_FAC_TYP_IND,'-',DEST_FAC_TYP_IND) IN ('STR-BDC', 'STR-FDC', 'STR-FDC/BDC')
  GROUP BY ALL
  HAVING LOADS >= 20
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.STR_SRS1_LANES` AS     
(
  SELECT null AS REGION,
        CAST(null AS STRING) AS CAMPUS_NBR_NM,
        CAST(null AS STRING) AS DC_NBR,
        A.SCAC,
        A.O_ID,
        A.O_TYPE,
        A.O_ZIP,
        A.O_CITY,
        A.O_STATE,
        srs.D_ID,
        srs.D_TYPE,
        srs.D_ZIP,
        srs.D_CITY,
        srs.D_STATE,
        'SSRS1' AS LEG,
        null AS DIST,
        0 AS LOADS,
        10000 AS OW_COST_PER_LOAD
  FROM `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND` A
  CROSS JOIN 
    (
      SELECT CONCAT('SRS_', Origin_ID) AS D_ID,
        'SRS1' AS D_TYPE,
        Origin_Zip AS D_ZIP,
        Origin_City AS D_CITY,
        Origin_State AS D_STATE
      FROM `analytics-supplychain-thd.SRS_Matching.SRS_LANES`
      WHERE 1=1
        AND Truckload_Volume >= 20
        AND Origin_ID IS NOT null 
        AND Dest_ID IS NOT null
        AND NOT REGEXP_CONTAINS(Origin_Zip, r"[A-Za-z]")
        AND NOT REGEXP_CONTAINS(Destination_Zip, r"[A-Za-z]")
    ) srs    
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.DC_SRS1_LANES` AS
(
  SELECT A.REGION,
        A.CAMPUS_NBR_NM,
        A.DC_NBR,
        A.SCAC,
        A.D_ID AS O_ID,
        A.D_TYPE AS O_TYPE,
        A.D_ZIP AS O_ZIP,
        A.D_CITY AS O_CITY,
        A.D_STATE AS O_STATE,
        srs.D_ID,
        srs.D_TYPE,
        srs.D_ZIP,
        srs.D_CITY,
        srs.D_STATE,
        'OSRS1' AS LEG,
        null AS DIST,
        0 AS LOADS,
        10000 AS OW_COST_PER_LOAD
  FROM `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND` A
  CROSS JOIN 
    (
      SELECT CONCAT('SRS_', Origin_ID) AS D_ID,
        'SRS1' AS D_TYPE,
        Origin_Zip AS D_ZIP,
        Origin_City AS D_CITY,
        Origin_State AS D_STATE
      FROM `analytics-supplychain-thd.SRS_Matching.SRS_LANES`
      WHERE 1=1
        AND Truckload_Volume >= 20
        AND Origin_ID IS NOT null 
        AND Dest_ID IS NOT null
        AND NOT REGEXP_CONTAINS(Origin_Zip, r"[A-Za-z]")
        AND NOT REGEXP_CONTAINS(Destination_Zip, r"[A-Za-z]")
    ) srs
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.SRS2_DC_LANES` AS
(
  SELECT A.REGION,
        A.CAMPUS_NBR_NM,
        A.DC_NBR,
        A.SCAC,
        srs.O_ID,
        srs.O_TYPE,
        srs.O_ZIP,
        srs.O_CITY,
        srs.O_STATE,
        A.D_ID,
        A.D_TYPE,
        A.D_ZIP,
        A.D_CITY,
        A.D_STATE,
        'SRS2O' AS LEG,
        null AS DIST,
        0 AS LOADS,
        10000 AS OW_COST_PER_LOAD
  FROM 
    (
      SELECT CONCAT('SRS_', Dest_ID) AS O_ID,
        'SRS2' AS O_TYPE,
        Destination_Zip AS O_ZIP,
        Destination_City AS O_CITY,
        Destination_State AS O_STATE
      FROM `analytics-supplychain-thd.SRS_Matching.SRS_LANES`
      WHERE 1=1
        AND Truckload_Volume >= 20
        AND Origin_ID IS NOT null 
        AND Dest_ID IS NOT null
        AND NOT REGEXP_CONTAINS(Origin_Zip, r"[A-Za-z]")
        AND NOT REGEXP_CONTAINS(Destination_Zip, r"[A-Za-z]")
    ) srs
  CROSS JOIN `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND` A
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` AS
(
  SELECT DISTINCT 
        REGION,
        CAMPUS_NBR_NM,
        DC_NBR,
        SCAC,
        O_ID,
        O_TYPE,
        O_ZIP,
        O_CITY,
        O_STATE,
        D_ID,
        D_TYPE,
        D_ZIP,
        D_CITY,
        D_STATE,
        LEG,
        DIST,
        0 AS LOADS,
        OW_COST_PER_LOAD,
        0 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND`

  UNION ALL

  SELECT DISTINCT 
        REGION,
        CAMPUS_NBR_NM,
        DC_NBR,
        SCAC,
        D_ID AS O_ID,
        D_TYPE AS O_TYPE,
        D_ZIP AS O_ZIP,
        D_CITY AS O_CITY,
        D_STATE AS O_STATE,
        O_ID AS D_ID,
        O_TYPE AS D_TYPE,
        O_ZIP AS D_ZIP,
        O_CITY AS D_CITY,
        O_STATE AS D_STATE,
        'OS',
        DIST,
        CEILING(LOADS/48) AS LOADS,
        OW_COST_PER_LOAD,
        1 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.STR_DC_EMPTY_DEMAND`

  UNION ALL

  SELECT DISTINCT 
        REGION,
        CAMPUS_NBR_NM,
        DC_NBR,
        SCAC,
        O_ID,
        O_TYPE,
        O_ZIP,
        O_CITY,
        O_STATE,
        D_ID,
        D_TYPE,
        CAST(D_ZIP AS STRING),
        D_CITY,
        D_STATE,
        LEG,
        DIST,
        LOADS,
        OW_COST_PER_LOAD,
        0 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.STR_SRS1_LANES`

  UNION ALL

  SELECT DISTINCT REGION,
        CAMPUS_NBR_NM,
        DC_NBR,
        SCAC,
        O_ID,
        O_TYPE,
        O_ZIP,
        O_CITY,
        O_STATE,
        D_ID,
        D_TYPE,
        CAST(D_ZIP AS STRING),
        D_CITY,
        D_STATE,
        LEG,
        DIST,
        LOADS,
        OW_COST_PER_LOAD,
        0 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.DC_SRS1_LANES`

  UNION ALL

  SELECT DISTINCT REGION,
        CAMPUS_NBR_NM,
        DC_NBR,
        SCAC,
        O_ID,
        O_TYPE,
        CAST(O_ZIP AS STRING) AS O_ZIP,
        O_CITY,
        O_STATE,
        D_ID,
        D_TYPE,
        D_ZIP,
        D_CITY,
        D_STATE,
        LEG,
        DIST,
        LOADS,
        OW_COST_PER_LOAD,
        0 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.SRS2_DC_LANES`

  UNION ALL      
------------- rounding down loads per week for SRS
  SELECT DISTINCT 
        null AS REGION,
        CAST(null AS STRING) AS CAMPUS_NBR_NM,
        CAST(null AS STRING) AS DC_NBR,
        'BSL' AS SCAC,
        CONCAT('SRS_',Origin_ID) AS O_ID,
        'SRS1' AS O_TYPE,
        CAST(Origin_Zip AS STRING) AS O_ZIP,
        Origin_City AS O_CITY,
        Origin_State AS O_STATE,
        CONCAT('SRS_',Dest_ID) AS D_ID,
        'SRS2' AS D_TYPE,
        CAST(Destination_Zip AS STRING) AS D_ZIP,
        Destination_City AS D_CITY,
        Destination_State AS D_STATE,
        'SRS1-SRS2' AS LEG,
        AVG(Distance) AS DIST,
        CEILING(MAX(Truckload_Volume)/40) AS LOADS,
        AVG(Low_Allin_Cost) AS OW_COST_PER_LOAD,
        1 AS LOADED_IND
  FROM `analytics-supplychain-thd.SRS_Matching.SRS_LANES`
  WHERE Truckload_Volume >= 20
    AND Origin_ID IS NOT null 
    AND Dest_ID IS NOT null
    AND NOT REGEXP_CONTAINS(Origin_Zip, r"[A-Za-z]")
    AND NOT REGEXP_CONTAINS(Destination_Zip, r"[A-Za-z]")
  GROUP BY ALL
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` AS
(
  SELECT *,
        CONCAT(O_TYPE,'-',D_TYPE) AS LEG_DTL,
        CASE WHEN LEG LIKE 'SRS%' THEN 'SRS'
              ELSE 'THD'
        END AS THD_SRS
  FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS`
);

UPDATE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` a
SET a.DIST = b.DIST
FROM (SELECT O_ZIP, D_ZIP, AVG(DIST) AS DIST
      FROM `analytics-supplychain-thd.FLEETO.FLEETO_LANES_DIST`
      GROUP BY 1,2) b
WHERE a.O_ZIP = b.O_ZIP
  AND a.D_ZIP = b.D_ZIP
  AND a.DIST IS null;

UPDATE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` a
SET a.DIST = b.DIST
FROM (SELECT O_ZIP, D_ZIP, AVG(DIST) AS DIST
      FROM `analytics-supplychain-thd.FLEETO.FLEETO_LANES_DIST`
      GROUP BY 1,2) b
WHERE a.O_ZIP = b.D_ZIP
  AND a.D_ZIP = b.O_ZIP
  AND a.DIST IS null;

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` AS
(
  WITH LONG_LAT AS (
    SELECT postal_code, max(longitude) long, max(latitude) lat 
    from `pr-edw-views-thd.SCHN_TRANS.T14_POSTAL_CODE`
    group by 1
  )
  ,dists AS (
    SELECT DISTINCT a.O_ZIP, a.D_ZIP, 
      b.long as O_long, b.lat as O_lat, 
      c.long as D_long, c.lat as D_lat,
      1.2*ST_DISTANCE(ST_GEOGPOINT(b.long, b.lat), 
                  ST_GEOGPOINT(c.long, c.lat))/1.6/1000 as GEO_DIST
    FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` a
    LEFT JOIN LONG_LAT b
    ON a.O_ZIP = b.postal_code
    LEFT JOIN LONG_LAT c
    ON a.D_ZIP = c.postal_code
  )
  SELECT a.*EXCEPT(DIST),
      COALESCE(DIST, GEO_DIST) AS DIST
  FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` a
  LEFT JOIN dists
    ON a.O_ZIP = dists.O_ZIP
    AND a.D_ZIP = dists.D_ZIP
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.Missing_Distances` AS 
(
  SELECT DISTINCT *
  FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS`
  WHERE DIST IS null OR DIST < 0
);

UPDATE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` a
SET a.DIST = b.DIST
FROM 
    (
    SELECT O_ID, D_ID, O_ZIP, D_ZIP, AVG(DIST) AS DIST 
    FROM `analytics-supplychain-thd.SRS_Matching.SRS_Missing_Distances`
    GROUP BY ALL
    ) b
WHERE a.O_ID = b.O_ID
    AND a.D_ID = b.D_ID
    AND CAST(a.O_ZIP AS STRING) = CAST(b.O_ZIP AS STRING)
    AND CAST(a.D_ZIP AS STRING) = CAST(b.D_ZIP AS STRING)
    AND a.DIST IS null
;

SELECT CAST(CASE WHEN MISS_DIST = 0 THEN "0" ELSE "MISSING DISTANCES" END AS INT64)
FROM 
    (
        SELECT COUNT(*) AS MISS_DIST
        FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS`
        WHERE DIST IS null  
    );

UPDATE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` A
SET A.OW_COST_PER_LOAD = B.BSL_Rate
FROM `analytics-supplychain-thd.SRS_Matching.SRS_RATES` B
WHERE A.O_ZIP = B.OZIP
    AND A.D_ZIP = B.DZIP;

DELETE `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS`
WHERE DIST < 0 OR DIST IS null
    OR OW_COST_PER_LOAD IS null;

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS` AS
(
  SELECT DISTINCT
        O_ID,
        O_TYPE,
        O_ZIP,
        D_ID,
        D_TYPE,
        D_ZIP,
        LEG,
        LEG_DTL,
        LOADED_IND,
        DIST
  FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS`
--   WHERE DIST <= 1200
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` AS
(
  SELECT A.*,
      ROW_NUMBER() OVER(ORDER BY O_ID, D_ID, O_ZIP, D_ZIP) AS PAIR_NUMBER
  FROM 
    (
      SELECT DISTINCT O_ID, D_ID, O_ZIP, D_ZIP
      FROM `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS`
    ) A
);

---demand table
CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` AS
(
  SELECT THD_SRS, 
        A.O_ID,
        A.O_TYPE,
        A.O_ZIP,
        UPPER(A.O_CITY) AS O_CITY,
        A.O_STATE,
        A.D_ID,
        A.D_TYPE,
        A.D_ZIP,
        UPPER(A.D_CITY) AS D_CITY,
        A.D_STATE,
        B.PAIR_NUMBER,
        AVG(A.OW_COST_PER_LOAD) AS OW_COST_PER_LOAD,
        A.LEG,
        AVG(A.DIST) AS DIST,
        FLOOR(AVG(A.LOADS)) AS LOADS,
        CASE WHEN LEG LIKE 'SRS%' THEN 'OW'
              ELSE 'DED'
        END AS MAJORITY_MODE
  FROM `analytics-supplychain-thd.SRS_Matching.INPUT_LEGS` A      
  LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` B   
    ON A.O_ID = B.O_ID
    AND A.D_ID = B.D_ID
    AND A.O_ZIP = B.O_ZIP
    AND A.D_ZIP = B.D_ZIP
  WHERE A.LOADED_IND = 1
  GROUP BY ALL
);

----Get all input legs
---form routes
