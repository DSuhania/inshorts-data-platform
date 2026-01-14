CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_ROLLING_ACTIVE_USERS("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_ROLLING_ACTIVE_USERS'';
var tgtTable = ''INSHORTS_DB.REPORTING.ROLLING_ACTIVE_USERS'';
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
      4. LOAD DATA
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            INSERT INTO ${tgtTable}
            SELECT
                d.event_date,

                COUNT(DISTINCT CASE
                    WHEN f.event_date BETWEEN DATEADD(day, -6, d.event_date) AND d.event_date
                    THEN f.deviceid
                END) AS wau,

                COUNT(DISTINCT CASE
                    WHEN f.event_date BETWEEN DATEADD(day, -29, d.event_date) AND d.event_date
                    THEN f.deviceid
                END) AS mau,

                ROUND(
                    COUNT(DISTINCT CASE
                        WHEN f.event_date BETWEEN DATEADD(day, -6, d.event_date) AND d.event_date
                        THEN f.deviceid
                    END)
                    /
                    NULLIF(
                        COUNT(DISTINCT CASE
                            WHEN f.event_date BETWEEN DATEADD(day, -29, d.event_date) AND d.event_date
                            THEN f.deviceid
                        END),
                        0
                    ),
                    2
                ) AS wau_mau_ratio,

                ''${BATCH_ID}''     AS batch_id,
                CURRENT_TIMESTAMP AS created_ts

            FROM (
                SELECT DISTINCT event_date
                FROM INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT
            ) d
            LEFT JOIN INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT f
              ON f.event_date BETWEEN DATEADD(day, -29, d.event_date) AND d.event_date
            GROUP BY d.event_date
            ORDER BY d.event_date
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
