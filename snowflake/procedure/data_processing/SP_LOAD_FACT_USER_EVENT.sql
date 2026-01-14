CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_FACT_USER_EVENT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_FACT_USER_EVENT'';
var tgtTable = ''INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT'';
var rerunVal = 0;

try {

    /*-------------------------------------------------
      1. CALCULATE RERUN (batch_id + target_table)
    -------------------------------------------------*/
    var rerunRs = snowflake.createStatement({
        sqlText: `
            SELECT COALESCE(MAX(rerun), -1) + 1
            FROM INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            WHERE batch_id = ''${BATCH_ID}''
              AND target_table = ''${tgtTable}''
        `
    }).execute();

    if (rerunRs.next()) {
        rerunVal = rerunRs.getColumnValue(1);
    }

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
      3. FACT LOAD (SURROGATE KEY JOIN + LATE ARRIVAL)
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            INSERT INTO INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT (
                user_sk,
                content_sk,
                deviceid,
                content_id,
                event_ts,
                event_date,
                eventname,
                timespent,
                batch_id,
                created_ts
            )
            SELECT
                COALESCE(u.user_sk, -1),
                COALESCE(c.content_sk, -1),
                e.deviceid,
                e.content_id,
                TO_TIMESTAMP_NTZ(e.eventtimestamp/1000),
                DATE(TO_TIMESTAMP_NTZ(e.eventtimestamp/1000)),
                e.eventname,
                e.timespend,
                ''${BATCH_ID}'',
                CURRENT_TIMESTAMP
            FROM INSHORTS_DB.RAW_LANDING.EVENT_LANDING e
            LEFT JOIN INSHORTS_DB.INTEGRATION.INT_DIM_USER u
              ON u.deviceid = e.deviceid
            LEFT JOIN INSHORTS_DB.INTEGRATION.INT_DIM_CONTENT c
              ON c.content_id = e.content_id
        `
    }).execute();

    /*-------------------------------------------------
      4. AUDIT SUCCESS
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

    return ''SUCCESS | Batch='' + BATCH_ID +
           '' Target='' + tgtTable +
           '' Rerun='' + rerunVal;

} catch (err) {

    var errMsg = err.message
        ? err.message.replace(/''/g, '''')
        : ''UNKNOWN_ERROR'';

    /*-------------------------------------------------
      5. AUDIT FAILED
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

    return ''FAILED | Batch='' + BATCH_ID +
           '' Target='' + tgtTable +
           '' Rerun='' + rerunVal +
           '' Error='' + err.message;
}
';
