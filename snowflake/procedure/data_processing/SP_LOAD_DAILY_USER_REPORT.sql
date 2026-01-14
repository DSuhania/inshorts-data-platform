CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_DAILY_USER_REPORT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_DAILY_USER_REPORT'';
var tgtTable = ''INSHORTS_DB.REPORTING.DAILY_USER_REPORT'';
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
                event_date,

                COUNT(DISTINCT deviceid) AS dau,
                AVG(total_time_spent)    AS avg_time_spent,
                AVG(articles_read)       AS avg_articles_read,

                SUM(CASE 
                        WHEN total_time_spent > 600 OR articles_read >= 10 THEN 1
                        ELSE 0
                    END) AS power_users,

                SUM(CASE 
                        WHEN total_time_spent BETWEEN 120 AND 600
                          OR articles_read BETWEEN 3 AND 9 THEN 1
                        ELSE 0
                    END) AS active_users,

                SUM(CASE 
                        WHEN total_time_spent < 120 AND articles_read < 3 THEN 1
                        ELSE 0
                    END) AS casual_users,

                ''${BATCH_ID}''     AS batch_id,
                CURRENT_TIMESTAMP AS created_ts
            FROM INSHORTS_DB.ANALYTICS.DAILY_USER_ACTIVITY
            GROUP BY event_date
            ORDER BY event_date
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
