CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_USER_SEGMENT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_USER_SEGMENT'';
var tgtTable = ''INSHORTS_DB.REPORTING.USER_SEGMENT'';
var rerunVal = 0;

try {

    // 1️⃣ Calculate rerun
    var rs = snowflake.createStatement({
        sqlText: `
            SELECT COALESCE(MAX(rerun), -1) + 1
            FROM INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            WHERE batch_id = ''${BATCH_ID}''
              AND target_table = ''${tgtTable}''
        `
    }).execute();
    if (rs.next()) rerunVal = rs.getColumnValue(1);

    // 2️⃣ Audit start
    snowflake.createStatement({
        sqlText: `
            INSERT INTO INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            (batch_id, procedure_name, target_table, rerun,
             load_status, is_error, load_date, start_ts)
            VALUES (
                ''${BATCH_ID}'', ''${procName}'', ''${tgtTable}'', ${rerunVal},
                ''STARTED'', FALSE, CURRENT_DATE, CURRENT_TIMESTAMP
            )
        `
    }).execute();

    // 3️⃣ Truncate target
    snowflake.createStatement({ sqlText: `TRUNCATE TABLE ${tgtTable}` }).execute();

    // 4️⃣ Load USER_SEGMENT
    snowflake.createStatement({
        sqlText: `
            INSERT INTO ${tgtTable} (DEVICEID, USER_SEGMENT, batch_id, created_ts)
            SELECT
                u.deviceid,
                CASE
                    WHEN SUM(f.timespent) > 600 THEN ''Power User''
                    WHEN SUM(f.timespent) BETWEEN 120 AND 600 THEN ''Active User''
                    ELSE ''Casual User''
                END AS user_segment,
                ''${BATCH_ID}'' AS batch_id,
                CURRENT_TIMESTAMP AS created_ts
            FROM INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT f
            JOIN INSHORTS_DB.INTEGRATION.INT_DIM_USER u
              ON f.deviceid = u.deviceid
            GROUP BY u.deviceid
        `
    }).execute();

    // 5️⃣ Audit success
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            SET load_status=''SUCCESS'', is_error=FALSE, end_ts=CURRENT_TIMESTAMP
            WHERE batch_id=''${BATCH_ID}'' AND target_table=''${tgtTable}'' AND rerun=${rerunVal}
        `
    }).execute();

    return ''SUCCESS | '' + tgtTable + '' | Batch='' + BATCH_ID + '' | Rerun='' + rerunVal;

} catch (err) {

    var errMsg = err.message.replace(/''/g,'''');

    // 6️⃣ Audit failed
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.DATA_LOAD_AUDIT
            SET load_status=''FAILED'', is_error=TRUE,
                error_message=''${errMsg}'', end_ts=CURRENT_TIMESTAMP
            WHERE batch_id=''${BATCH_ID}'' AND target_table=''${tgtTable}'' AND rerun=${rerunVal}
        `
    }).execute();

    return ''FAILED | '' + tgtTable + '' | '' + errMsg;
}
';
