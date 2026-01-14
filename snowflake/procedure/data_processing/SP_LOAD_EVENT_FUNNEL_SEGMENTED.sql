CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_EVENT_FUNNEL_SEGMENTED("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_EVENT_FUNNEL_SEGMENTED'';
var tgtTable = ''INSHORTS_DB.REPORTING.EVENT_FUNNEL_SEGMENTED'';
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
                f.event_date,
                s.user_segment,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Shown''  THEN f.deviceid END) AS shown_users,
                COUNT(DISTINCT CASE WHEN f.eventname = ''Opened'' THEN f.deviceid END) AS opened_users,
                COUNT(DISTINCT CASE WHEN f.eventname = ''Front''  THEN f.deviceid END) AS front_read_users,
                COUNT(DISTINCT CASE WHEN f.eventname = ''Back''   THEN f.deviceid END) AS back_read_users,
                COUNT(DISTINCT CASE WHEN f.eventname = ''Shared'' THEN f.deviceid END) AS shared_users,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Opened'' THEN f.deviceid END)
                / NULLIF(
                    COUNT(DISTINCT CASE WHEN f.eventname = ''Shown'' THEN f.deviceid END),
                    0
                ) AS shown_to_opened_ctr,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Front'' THEN f.deviceid END)
                / NULLIF(
                    COUNT(DISTINCT CASE WHEN f.eventname = ''Opened'' THEN f.deviceid END),
                    0
                ) AS opened_to_front_rate,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Back'' THEN f.deviceid END)
                / NULLIF(
                    COUNT(DISTINCT CASE WHEN f.eventname = ''Front'' THEN f.deviceid END),
                    0
                ) AS front_to_back_rate,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Back'' THEN f.deviceid END)
                / NULLIF(
                    COUNT(DISTINCT CASE WHEN f.eventname = ''Opened'' THEN f.deviceid END),
                    0
                ) AS deep_read_rate,

                COUNT(DISTINCT CASE WHEN f.eventname = ''Shared'' THEN f.deviceid END)
                / NULLIF(
                    COUNT(DISTINCT CASE WHEN f.eventname = ''Front'' THEN f.deviceid END),
                    0
                ) AS share_rate,

                ''${BATCH_ID}''     AS batch_id,
                CURRENT_TIMESTAMP AS created_ts

            FROM INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT f
            JOIN INSHORTS_DB.ANALYTICS.USER_SEGMENT s
              ON f.deviceid = s.deviceid
            GROUP BY
                f.event_date,
                s.user_segment
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
