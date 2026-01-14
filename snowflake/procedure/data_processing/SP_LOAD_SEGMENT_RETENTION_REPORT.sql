CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_SEGMENT_RETENTION_REPORT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_SEGMENT_RETENTION_REPORT'';
var tgtTable = ''INSHORTS_DB.REPORTING.SEGMENT_RETENTION_REPORT'';
var rerunVal = 0;

try {

    /*-------------------------------------------------
      1. CALCULATE RERUN
    -------------------------------------------------*/
    var rs = snowflake.createStatement({
        sqlText: `
            SELECT COALESCE(MAX(rerun), -1) + 1
            FROM INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            WHERE batch_id = ''${BATCH_ID}''
              AND target_table = ''${tgtTable}''
        `
    }).execute();

    if (rs.next()) rerunVal = rs.getColumnValue(1);

    /*-------------------------------------------------
      2. AUDIT START
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            INSERT INTO INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            (batch_id, procedure_name, target_table, rerun,
             load_status, is_error, load_date, start_ts)
            VALUES (
                ''${BATCH_ID}'',
                ''${procName}'',
                ''${tgtTable}'',
                ${rerunVal},
                ''STARTED'',
                FALSE,
                CURRENT_DATE,
                CURRENT_TIMESTAMP
            )
        `
    }).execute();

    /*-------------------------------------------------
      3. TRUNCATE TARGET
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `TRUNCATE TABLE ${tgtTable}`
    }).execute();

    /*-------------------------------------------------
      4. LOAD SEGMENT RETENTION (COHORT-CORRECT)
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            INSERT INTO ${tgtTable}
            WITH cohort AS (
                SELECT
                    u.install_dt,
                    s.user_segment,
                    u.deviceid
                FROM INSHORTS_DB.INTEGRATION.INT_DIM_USER u
                JOIN INSHORTS_DB.REPORTING.USER_SEGMENT s
                  ON u.deviceid = s.deviceid
            ),

            cohort_size AS (
                SELECT
                    install_dt,
                    user_segment,
                    COUNT(DISTINCT deviceid) AS cohort_users
                FROM cohort
                GROUP BY install_dt, user_segment
            ),

            activity AS (
                SELECT DISTINCT
                    c.install_dt,
                    c.user_segment,
                    c.deviceid,
                    DATEDIFF(day, c.install_dt, f.event_date) AS day_number
                FROM cohort c
                JOIN INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT f
                  ON c.deviceid = f.deviceid
                WHERE DATEDIFF(day, c.install_dt, f.event_date) IN (1, 7, 30)
            )

            SELECT
                cs.install_dt,
                cs.user_segment,
                cs.cohort_users,

                ROUND(
                    100 * COUNT(DISTINCT CASE WHEN a.day_number = 1  THEN a.deviceid END)
                    / cs.cohort_users, 2
                ) AS d1_retention_pct,

                ROUND(
                    100 * COUNT(DISTINCT CASE WHEN a.day_number = 7  THEN a.deviceid END)
                    / cs.cohort_users, 2
                ) AS w1_retention_pct,

                ROUND(
                    100 * COUNT(DISTINCT CASE WHEN a.day_number = 30 THEN a.deviceid END)
                    / cs.cohort_users, 2
                ) AS m1_retention_pct,

                ''${BATCH_ID}''     AS batch_id,
                CURRENT_TIMESTAMP AS created_ts

            FROM cohort_size cs
            LEFT JOIN activity a
              ON cs.install_dt   = a.install_dt
             AND cs.user_segment = a.user_segment
            GROUP BY
                cs.install_dt,
                cs.user_segment,
                cs.cohort_users
            ORDER BY
                cs.install_dt,
                cs.user_segment
        `
    }).execute();

    /*-------------------------------------------------
      5. AUDIT SUCCESS
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            SET load_status = ''SUCCESS'',
                is_error    = FALSE,
                end_ts      = CURRENT_TIMESTAMP
            WHERE batch_id = ''${BATCH_ID}''
              AND target_table = ''${tgtTable}''
              AND rerun = ${rerunVal}
        `
    }).execute();

    return ''SUCCESS | '' + tgtTable + '' | Batch='' + BATCH_ID + '' | Rerun='' + rerunVal;

} catch (err) {

    var errMsg = err.message.replace(/''/g,'''');

    /*-------------------------------------------------
      6. AUDIT FAILED
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            SET load_status   = ''FAILED'',
                is_error      = TRUE,
                error_message = ''${errMsg}'',
                end_ts        = CURRENT_TIMESTAMP
            WHERE batch_id = ''${BATCH_ID}''
              AND target_table = ''${tgtTable}''
              AND rerun = ${rerunVal}
        `
    }).execute();

    return ''FAILED | '' + tgtTable + '' | '' + errMsg;
}
';
