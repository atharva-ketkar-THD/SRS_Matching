--------
CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.ROUTES_LANES` AS
(
    SELECT A.*, B.*EXCEPT(ROUTE_NBR, MODE, HOME_DC), 
        C1.O_CITY AS A_OCITY, C1.D_CITY AS A_DCITY, C1.O_STATE AS A_OSTATE, C1.D_STATE AS A_DSTATE,
        C2.O_CITY AS B_OCITY, C2.D_CITY AS B_DCITY, C2.O_STATE AS B_OSTATE, C2.D_STATE AS B_DSTATE,
        C3.O_CITY AS C_OCITY, C3.D_CITY AS C_DCITY, C3.O_STATE AS C_OSTATE, C3.D_STATE AS C_DSTATE,
        C4.O_CITY AS D_OCITY, C4.D_CITY AS D_DCITY, C4.O_STATE AS D_OSTATE, C4.D_STATE AS D_DSTATE,
        C1.OW_COST_PER_LOAD AS LEG1_OW_COST,
        C2.OW_COST_PER_LOAD AS LEG2_OW_COST,
        C3.OW_COST_PER_LOAD AS LEG3_OW_COST,
        C4.OW_COST_PER_LOAD AS LEG4_OW_COST
    FROM `analytics-supplychain-thd.SRS_Matching.SRS_MATCHING_DED` A       
    LEFT JOIN 
        (
            SELECT *,
                SAFE_DIVIDE((IFNULL(A_LOADED_IND*A_DIST,0) +
                IFNULL(B_LOADED_IND*B_DIST,0) +
                IFNULL(C_LOADED_IND*C_DIST,0) +
                IFNULL(D_LOADED_IND*D_DIST,0)
                ),TOTAL_DIST) AS LOADED_PCT
            FROM `analytics-supplychain-thd.SRS_Matching.rf_routes_final`
        ) B    
        ON A.ROUTE_NBR = B.ROUTE_NBR 
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` C1
        ON B.PAIR_NBR1 = C1.PAIR_NUMBER
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` C2
        ON B.PAIR_NBR2 = C2.PAIR_NUMBER
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` C3
        ON B.PAIR_NBR3 = C3.PAIR_NUMBER
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` C4
        ON B.PAIR_NBR4 = C4.PAIR_NUMBER
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS` AS
(
    SELECT *,
        CASE
            WHEN A_LOADED_IND = 1 THEN 2*SAFE_DIVIDE(A_DIST*COST,TOTAL_DIST)
            ELSE 0
        END AS BASELINE_COST,
        CASE WHEN A_LEG LIKE 'SRS1-SRS2' THEN LEG1_OW_COST ELSE 0 END +
        CASE WHEN B_LEG LIKE 'SRS1-SRS2' THEN LEG2_OW_COST ELSE 0 END +
        CASE WHEN C_LEG LIKE 'SRS1-SRS2' THEN LEG3_OW_COST ELSE 0 END +
        CASE WHEN D_LEG LIKE 'SRS1-SRS2' THEN LEG4_OW_COST ELSE 0 END AS SRS_CREDIT
    FROM `analytics-supplychain-thd.SRS_Matching.ROUTES_LANES`
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS` AS
(
    SELECT HOME_DC, campus.CAMPUS_NBR_NM, campus.SCAC,
        ROUTE_NBR, LOADS AS DED_LOADS, 
        MODE, TOTAL_DIST,
        A_ORIGIN, A_DEST, A_LEG, A_LEG_DTL, A_DIST, LEG1_OW_COST, A_OZIP, A_DZIP, A_OCITY, A_DCITY, A_OSTATE, A_DSTATE, A_LOADED_IND,
        B_ORIGIN, B_DEST, B_LEG, B_LEG_DTL, B_DIST, LEG2_OW_COST, B_OZIP, B_DZIP, B_OCITY, B_DCITY, B_OSTATE, B_DSTATE, B_LOADED_IND,
        C_ORIGIN, C_DEST, C_LEG, C_LEG_DTL, C_DIST, LEG3_OW_COST, C_OZIP, C_DZIP, C_OCITY, C_DCITY, C_OSTATE, C_DSTATE, C_LOADED_IND,
        D_ORIGIN, D_DEST, D_LEG, D_LEG_DTL, D_DIST, LEG4_OW_COST, D_OZIP, D_DZIP, D_OCITY, D_DCITY, D_OSTATE, D_DSTATE, D_LOADED_IND,
        PAIR_NBR1, PAIR_NBR2, PAIR_NBR3, PAIR_NBR4, LOADED_PCT,
        BASELINE_COST, SRS_CREDIT, RF_COST,
        BASELINE_COST - (RF_COST - SRS_CREDIT) AS SAVINGS_PER_LOAD,
        (BASELINE_COST - (RF_COST - SRS_CREDIT))*LOADS AS TOTAL_SAVINGS_PER_WEEK,
        RF_COST - SRS_CREDIT AS EFFECTIVE_RT_COST,
        TOTAL_DIST - 2*A_DIST AS OOR_DIST
    FROM `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS` a
    LEFT JOIN `analytics-supplychain-thd.LAB_TRANS_ANALYTICS.FLATBED_CAMPUS_MAPPING` campus
        ON a.HOME_DC = campus.CAMPUS_NAME
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS` AS   
(
    SELECT *,
        CONCAT
            (
                IFNULL(A_ORIGIN,''), CASE WHEN A_LOADED_IND=1 THEN '->' 
                                            WHEN A_LOADED_IND=0 THEN '-' ELSE '' END,
                IFNULL(A_DEST,''), CASE WHEN B_LOADED_IND=1 THEN '->' 
                                            WHEN B_LOADED_IND=0 THEN '-' ELSE '' END,
                IFNULL(B_DEST,''), CASE WHEN C_LOADED_IND=1 THEN '->' 
                                            WHEN C_LOADED_IND=0 THEN '-' ELSE '' END,
                IFNULL(C_DEST,''), CASE WHEN D_LOADED_IND=1 THEN '->' 
                                            WHEN D_LOADED_IND=0 THEN '-' ELSE '' END,
                IFNULL(D_DEST,'')
            ) AS ROUTE_DTL
        , SAFE_DIVIDE(SAVINGS_PER_LOAD, BASELINE_COST) AS SAVINGS_PCT
    FROM `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS`
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.PAIR_TO_ROUTE_LOOKUP` AS
(
    WITH DETAILS AS      
        (
        SELECT *
        FROM `analytics-supplychain-thd.SRS_Matching.ROUTES_SAVINGS`
        )
    ,PAIR_LOADS AS     
        (
        SELECT *
        FROM
            (
            SELECT PAIR_NBR1, ROUTE_NBR, DED_LOADS, SAVINGS_PER_LOAD, EFFECTIVE_RT_COST, LOADED_PCT, SAVINGS_PCT, OOR_DIST
            FROM DETAILS
            WHERE A_LOADED_IND = 1
            UNION ALL
            SELECT PAIR_NBR2, ROUTE_NBR, DED_LOADS, SAVINGS_PER_LOAD, EFFECTIVE_RT_COST, LOADED_PCT, SAVINGS_PCT, OOR_DIST
            FROM DETAILS
            WHERE B_LOADED_IND = 1
            UNION ALL
            SELECT PAIR_NBR3, ROUTE_NBR, DED_LOADS, SAVINGS_PER_LOAD, EFFECTIVE_RT_COST, LOADED_PCT, SAVINGS_PCT, OOR_DIST
            FROM DETAILS
            WHERE C_LOADED_IND = 1
            UNION ALL
            SELECT PAIR_NBR4, ROUTE_NBR, DED_LOADS, SAVINGS_PER_LOAD, EFFECTIVE_RT_COST, LOADED_PCT, SAVINGS_PCT, OOR_DIST
            FROM DETAILS
            WHERE D_LOADED_IND = 1
            )
        WHERE PAIR_NBR1 IS NOT NULL
        )
    
    SELECT *
    FROM PAIR_LOADS
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.ROUTE_TO_PAIR_LOOKUP_BASE` AS
(
   SELECT PAIR_NBR1,
        STRING_AGG(CAST(ROUTE_NBR AS STRING), ', ') AS ROUTE_NBRS,
        SUM(DED_LOADS) AS MODEL_DED_LOADS,
        SAFE_DIVIDE(SUM(OOR_DIST*DED_LOADS), SUM(DED_LOADS)) AS AVG_OOR_MILES,
        AVG(EFFECTIVE_RT_COST) AS AVG_EFFECTIVE_RT_COST,
        AVG(SAVINGS_PER_LOAD) AS AVG_SAVINGS_PER_LOAD,
        AVG(LOADED_PCT) AS AVG_LOADED_PCT,
        SAFE_DIVIDE(SUM(DED_LOADS*SAVINGS_PCT),SUM(DED_LOADS)) AS AVG_SAVINGS_PCT
    FROM `analytics-supplychain-thd.SRS_Matching.PAIR_TO_ROUTE_LOOKUP`
    GROUP BY PAIR_NBR1
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.LANES_ON_ROUTES` AS
(
    SELECT A.THD_SRS, A.PAIR_NUMBER,
        A.O_ID, A.O_TYPE, A.O_ZIP, A.O_CITY, A.O_STATE, 
        A.D_ID, A.D_TYPE, A.D_ZIP, A.D_CITY, A.D_STATE,
        A.LEG, A.DIST, 
        A.LOADS AS MODEL_LOADS, COALESCE(B.MODEL_DED_LOADS,0) AS MODEL_DED_LOADS,
        A.LOADS - COALESCE(B.MODEL_DED_LOADS,0) AS MODEL_OW_LOADS,
        A.OW_COST_PER_LOAD,
        B.ROUTE_NBRS, B.AVG_EFFECTIVE_RT_COST, B.AVG_SAVINGS_PER_LOAD, B.AVG_LOADED_PCT, B.AVG_SAVINGS_PCT
    FROM `analytics-supplychain-thd.SRS_Matching.DEMAND_DATA` A
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.ROUTE_TO_PAIR_LOOKUP_BASE` B
        ON A.PAIR_NUMBER = B.PAIR_NBR1
);


