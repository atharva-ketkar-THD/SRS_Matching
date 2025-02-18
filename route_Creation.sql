CREATE TEMP TABLE `CAMPUS_MAPPING` AS
(
    SELECT DISTINCT CAMPUS_NAME AS DC  
    FROM `analytics-supplychain-thd.LAB_TRANS_ANALYTICS.FLATBED_CAMPUS_MAPPING`
);

CREATE TEMP TABLE `TWO_LEGS_COMBOS` AS
(
    SELECT A.O_ID AS a_origin,
        A.D_ID AS a_dest,
        A.LEG AS a_leg,
        A.DIST AS a_dist,
        A.LEG_DTL AS a_leg_dtl,
        A.O_ZIP AS a_ozip,
        A.D_ZIP AS a_dzip,
        A.LOADED_IND AS a_loaded_ind,
        B.O_ID AS b_origin,
        B.D_ID AS b_dest,
        B.LEG AS b_leg,
        B.DIST AS b_dist,
        B.LEG_DTL AS b_leg_dtl,
        B.O_ZIP AS b_ozip,
        B.D_ZIP AS b_dzip,
        B.LOADED_IND AS b_loaded_ind   
    FROM 
        (
            SELECT *
            FROM `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS`
        ) A      
    JOIN `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS` B   
        ON A.D_ID = B.O_ID
        AND A.LOADED_IND + B.LOADED_IND > 0
        AND A.DIST + B.DIST <= 1200
    GROUP BY ALL
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.TWO_LEG_ROUTES` AS
(
    SELECT A.*,
       cast(NULL AS STRING) AS c_origin,
        cast(NULL AS STRING) AS c_dest,
        cast(NULL AS STRING) AS c_leg,
        cast(NULL AS float64) AS c_dist,
        cast(NULL AS STRING) AS c_leg_dtl,
        cast(NULL AS STRING) AS c_ozip,
        cast(NULL as STRING) AS c_dzip,
        cast(NULL as INT64) AS c_loaded_ind,
        
        cast(NULL AS STRING) AS d_origin,
        cast(NULL AS STRING) AS d_dest,
        cast(NULL AS STRING) AS d_leg,
        cast(NULL AS float64) AS d_dist,
        cast(NULL AS STRING) AS d_leg_dtl,
        cast(NULL AS STRING) AS d_ozip,
        cast(NULL as STRING) AS d_dzip,
        cast(NULL as INT64) AS d_loaded_ind,
        1 AS DC_Count,
        a_dist + b_dist AS Total_Dist 
    FROM `pr-supply-chain-thd.TEMP_BQA.TWO_LEGS_COMBOS_TASK_ID_113394` A
    WHERE a_origin = b_dest
        AND a_origin IN (SELECT DISTINCT DC
                        FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`)
);

CREATE TEMP TABLE `THREE_LEGS_COMBOS` AS
(
    SELECT A.a_origin,
        A.a_dest,
        A.a_leg,
        A.a_dist,
        A.a_leg_dtl,
        A.a_ozip,
        A.a_dzip,
        A.a_loaded_ind,
        A.b_origin,
        A.b_dest,
        A.b_leg,
        A.b_dist,
        A.b_leg_dtl,
        A.b_ozip,
        A.b_dzip,
        A.b_loaded_ind,
        C.O_ID AS c_origin,
        C.D_ID AS c_dest,
        C.LEG AS c_leg,
        C.DIST AS c_dist,
        C.LEG_DTL AS c_leg_dtl,
        C.O_ZIP AS c_ozip,
        C.D_ZIP AS c_dzip,
        C.LOADED_IND AS c_loaded_ind
    FROM 
        (
            SELECT
                a_origin, a_dest, a_leg, a_leg_dtl, a_ozip, a_dzip, a_dist, a_loaded_ind,
                b_origin, b_dest, b_leg, b_leg_dtl, b_ozip, b_dzip, b_dist, b_loaded_ind
            FROM `pr-supply-chain-thd.TEMP_BQA.TWO_LEGS_COMBOS_TASK_ID_113394`
            WHERE a_origin <> b_dest 
        ) A       
    JOIN `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS` C      
        ON C.O_ID = A.b_dest
        AND A.b_loaded_ind + C.LOADED_IND > 0
        AND C.O_ID NOT IN (a_origin, b_origin, a_dest)
        AND C.D_ID NOT IN (a_dest, b_dest, b_origin)
    WHERE A.a_dist + A.b_dist + C.DIST <= 1200
    GROUP BY ALL
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.THREE_LEG_ROUTES` AS
(
    SELECT a_origin, a_dest, a_leg, a_dist,
        a_leg_dtl, a_ozip, a_dzip, a_loaded_ind,
        b_origin, b_dest, b_leg, b_dist,
        b_leg_dtl, b_ozip, b_dzip, b_loaded_ind,
        c_origin,
        c_dest,
        c_leg,
        c_dist,
        c_leg_dtl,
        c_ozip,
        c_dzip,
        c_loaded_ind,
        cast(NULL AS STRING) AS d_origin,
        cast(NULL AS STRING) AS d_dest,
        cast(NULL AS STRING) AS d_leg,
        cast(NULL AS float64) AS d_dist,
        cast(NULL AS STRING) AS d_leg_dtl,
        cast(NULL AS STRING) AS d_ozip,
        cast(NULL as STRING) AS d_dzip,
        cast(NULL as INT64) AS d_loaded_ind,
        CASE
            WHEN a_dest IN (SELECT DISTINCT DC 
                             FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) 
            THEN 1
            ELSE 0
        END + 
        CASE
            WHEN b_dest IN (SELECT DISTINCT DC 
                            FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) 
            THEN 1
            ELSE 0
        END + 1 AS DC_Count,
        a_dist + b_dist + c_dist AS Total_Dist
    FROM `pr-supply-chain-thd.TEMP_BQA.THREE_LEGS_COMBOS_TASK_ID_113394`
    WHERE a_origin = c_dest
    AND a_origin IN (SELECT DISTINCT DC
                    FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`)
    AND (
            (CASE WHEN a_loaded_ind = 0 THEN a_dist ELSE 0 END +
            CASE WHEN b_loaded_ind = 0 THEN b_dist ELSE 0 END +
            CASE WHEN c_loaded_ind = 0 THEN  c_dist ELSE 0 END) /(a_dist + b_dist + c_dist)
        ) < 0.50
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.FOUR_LEG_ROUTES` AS
(
    SELECT a_origin, a_dest, a_leg, a_dist,
        a_leg_dtl, a_ozip, a_dzip, a_loaded_ind,
        b_origin, b_dest, b_leg, b_dist,
        b_leg_dtl, b_ozip, b_dzip, b_loaded_ind,
        c_origin, c_dest, c_leg, c_dist,
        c_leg_dtl, c_ozip, c_dzip, c_loaded_ind,
        B.O_ID AS d_origin,
        B.D_ID AS d_dest,
        B.LEG AS d_leg,
        B.DIST AS d_dist,
        B.LEG_DTL AS d_leg_dtl,
        B.O_ZIP AS d_ozip,
        B.D_ZIP AS d_dzip,
        B.LOADED_IND AS d_loaded_ind,
        CASE
            WHEN a_dest IN (SELECT DISTINCT DC 
                             FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) 
            THEN 1
            ELSE 0
        END + 
        CASE
            WHEN b_dest IN (SELECT DISTINCT DC 
                            FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) 
            THEN 1
            ELSE 0
        END + 
        CASE
            WHEN c_dest IN (SELECT DISTINCT DC 
                            FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) 
            THEN 1
            ELSE 0
        END + 1 AS DC_Count,
        a_dist + b_dist + c_dist + B.dist AS Total_Dist
    FROM `pr-supply-chain-thd.TEMP_BQA.THREE_LEGS_COMBOS_TASK_ID_113394` A
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_LOCATIONS` B    
        ON A.c_dest = B.O_ID
        AND A.a_origin = B.D_ID 
        AND A.c_loaded_ind + B.LOADED_IND > 0
        AND B.O_ID NOT IN (a_origin, b_origin, c_origin, a_dest, b_dest)
        AND B.D_ID NOT IN (a_dest, b_dest, c_dest, b_origin, c_origin)
    WHERE a_origin IN (SELECT DISTINCT DC
                        FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`)
        AND A.a_origin <> A.c_dest
        AND a_dist + b_dist + c_dist + B.DIST <= 1200
        AND CONCAT(a_leg, b_leg, c_leg, B.LEG) LIKE '%SRS%'
        AND (
            (CASE WHEN a_loaded_ind = 0 THEN a_dist ELSE 0 END +
            CASE WHEN b_loaded_ind = 0 THEN b_dist ELSE 0 END +
            CASE WHEN c_loaded_ind = 0 THEN  c_dist ELSE 0 END + 
            CASE WHEN B.loaded_ind = 0 THEN  B.dist ELSE 0 END) /(a_dist + b_dist + c_dist + B.dist)
        ) < 0.50
        AND (
            CASE
                WHEN a_dest IN (SELECT DISTINCT DC FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) THEN 1
                ELSE 0
            END + 
            CASE
                WHEN b_dest IN (SELECT DISTINCT DC FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) THEN 1
                ELSE 0
            END + 
            CASE
                WHEN c_dest IN (SELECT DISTINCT DC FROM `pr-supply-chain-thd.TEMP_BQA.CAMPUS_MAPPING_TASK_ID_113394`) THEN 1
                ELSE 0
            END
        ) < 3
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.rf_routes` AS
(
    SELECT *, 'D' AS MODE
    FROM `analytics-supplychain-thd.SRS_Matching.TWO_LEG_ROUTES`
    UNION ALL
    SELECT *, 'D' AS MODE
    FROM `analytics-supplychain-thd.SRS_Matching.THREE_LEG_ROUTES`
    UNION ALL
    SELECT *, 'D' AS MODE
    FROM `analytics-supplychain-thd.SRS_Matching.FOUR_LEG_ROUTES`
);

DELETE `analytics-supplychain-thd.SRS_Matching.rf_routes` 
WHERE SPLIT(a_leg_dtl, '-')[0] NOT IN ('FDC', 'BDC', 'FDC/BDC', 'BDC/FDC');

------------ Rates

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.rf_routes_1` AS
(
    WITH DED_RATES AS 
    (
        SELECT DISTINCT CAST(cost.DC_NBR AS STRING) AS DC_NBR, 
            AVG(cost.Variable_CPM) AS VARIABLE_CPM,
            AVG(cost.Stop_Charge) AS STOP_CHARGE_PER_INCREMENT,
            1 AS AVERAGE_INCREMENT_COUNT,
            AVG(cost.Fixed_Cost_per_Hour) AS FIXED_COST_PER_HOUR,
            0.4 AS Fuel_CPM
        FROM `analytics-supplychain-thd.LAB_TRANS_ANALYTICS.FLATBED_DEDICATED_COST` cost
        GROUP BY ALL
    )
    SELECT A.*,
        COALESCE(
            a_dist*D.Variable_CPM +
            a_dist*D.Fuel_CPM +
            D.STOP_CHARGE_PER_INCREMENT +
            (CASE
                WHEN A.a_dist <= 50 THEN GREATEST(0.33, A.a_dist/30)
                WHEN A.a_dist > 50 AND A.a_dist <= 100 THEN 1.667+ (A.a_dist - 50)/50
                WHEN A.a_dist > 100 THEN 2.667 + (A.a_dist - 100)/65
            END)*D.FIXED_COST_PER_HOUR
        ,0) + 
        COALESCE(
            b_dist*D.Variable_CPM +
            b_dist*D.Fuel_CPM +
            D.STOP_CHARGE_PER_INCREMENT +
            (CASE
                WHEN A.b_dist <= 50 THEN GREATEST(0.33, A.b_dist/30)
                WHEN A.b_dist > 50 AND A.b_dist <= 100 THEN 1.667+ (A.b_dist - 50)/50
                WHEN A.b_dist > 100 THEN 2.667 + (A.b_dist - 100)/65
            END)*D.FIXED_COST_PER_HOUR
        ,0) +
        COALESCE(
            c_dist*D.Variable_CPM +
            c_dist*D.Fuel_CPM +
            D.STOP_CHARGE_PER_INCREMENT +
            (CASE
                WHEN A.c_dist <= 50 THEN GREATEST(0.33, A.c_dist/30)
                WHEN A.c_dist > 50 AND A.c_dist <= 100 THEN 1.667+ (A.c_dist - 50)/50
                WHEN A.c_dist > 100 THEN 2.667 + (A.c_dist - 100)/65
            END)*D.FIXED_COST_PER_HOUR
        ,0) +
        COALESCE(
            d_dist*D.Variable_CPM +
            d_dist*D.Fuel_CPM +
            D.STOP_CHARGE_PER_INCREMENT +
            (CASE
                WHEN A.d_dist <= 50 THEN GREATEST(0.33, A.d_dist/30)
                WHEN A.d_dist > 50 AND A.d_dist <= 100 THEN 1.667+ (A.d_dist - 50)/50
                WHEN A.d_dist > 100 THEN 2.667 + (A.d_dist - 100)/65
            END)*D.FIXED_COST_PER_HOUR
        ,0) AS RF_COST
    FROM `analytics-supplychain-thd.SRS_Matching.rf_routes` A
    LEFT JOIN DED_RATES D
        ON A.a_origin = D.DC_NBR
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.rf_routes_2` AS
(
    SELECT A.*
        ,Q1.PAIR_NUMBER AS PAIR_NBR1
        ,Q2.PAIR_NUMBER AS PAIR_NBR2
        ,Q3.PAIR_NUMBER AS PAIR_NBR3
        ,Q4.PAIR_NUMBER AS PAIR_NBR4
    FROM `analytics-supplychain-thd.SRS_Matching.rf_routes_1` A
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` Q1
        ON A.a_origin = Q1.O_ID
        AND A.a_dest = Q1.D_ID
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` Q2
        ON A.b_origin = Q2.O_ID
        AND A.b_dest = Q2.D_ID
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` Q3
        ON A.c_origin = Q3.O_ID
        AND A.c_dest = Q3.D_ID
    LEFT JOIN `analytics-supplychain-thd.SRS_Matching.RF_pair_number_lanes` Q4
        ON A.d_origin = Q4.O_ID
        AND A.d_dest = Q4.D_ID
);

CREATE OR REPLACE TABLE `analytics-supplychain-thd.SRS_Matching.rf_routes_final` AS
(
    SELECT 
        A_ORIGIN, B_ORIGIN, C_ORIGIN, D_ORIGIN, 
        A_DEST, B_DEST, C_DEST, D_DEST,
        A_LEG, B_LEG, C_LEG, D_LEG,
        A_LEG_DTL, B_LEG_DTL, C_LEG_DTL, D_LEG_DTL,
        A_DIST, B_DIST, C_DIST, D_DIST,
        A_OZIP, B_OZIP, C_OZIP, D_OZIP,
        A_DZIP, B_DZIP, C_DZIP, D_DZIP,
        A_LOADED_IND, B_LOADED_IND, C_LOADED_IND, D_LOADED_IND,
        PAIR_NBR1, PAIR_NBR2, PAIR_NBR3, PAIR_NBR4,
        DC_COUNT, TOTAL_DIST, RF_COST, MODE,
        A_ORIGIN AS HOME_DC,
        ROW_NUMBER() OVER() AS ROUTE_NBR
    FROM `analytics-supplychain-thd.SRS_Matching.rf_routes_2`
);



