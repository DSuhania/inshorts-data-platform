CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_INT_DIM_DATE("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_INT_DIM_DATE'';
var tgtTable = ''INSHORTS_DB.INTEGRATION.INT_DIM_DATE'';
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
      3. TRUNCATE TABLE
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `TRUNCATE TABLE ${tgtTable}`
    }).execute();

    /*-------------------------------------------------
      4. GET MIN AND MAX DATE FROM FACT TABLE
    -------------------------------------------------*/
    var dateRs = snowflake.createStatement({
        sqlText: `
            SELECT MIN(EVENT_DATE) AS start_date, MAX(EVENT_DATE) AS end_date
            FROM INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT
        `
    }).execute();

    dateRs.next();
    var start_date = dateRs.getColumnValue(1);  // JS Date object
    var end_date = dateRs.getColumnValue(2);

    // Convert JS Date to ''YYYY-MM-DD'' string for Snowflake
    var start_date_str = start_date.toISOString().slice(0,10);
    var end_date_str   = end_date.toISOString().slice(0,10);

    /*-------------------------------------------------
      5. INSERT DATES DYNAMICALLY
    -------------------------------------------------*/
    var insertSQL = `
        INSERT INTO ${tgtTable}
        SELECT 
            DATEADD(DAY, seq4(), DATE ''${start_date_str}'') AS date_key,
            YEAR(DATEADD(DAY, seq4(), DATE ''${start_date_str}'')) AS year,
            MONTH(DATEADD(DAY, seq4(), DATE ''${start_date_str}'')) AS month,
            WEEKOFYEAR(DATEADD(DAY, seq4(), DATE ''${start_date_str}'')) AS week,
            DAY(DATEADD(DAY, seq4(), DATE ''${start_date_str}'')) AS day,
            ''${BATCH_ID}'' AS batch_id,
            CURRENT_TIMESTAMP() AS created_ts
        FROM TABLE(GENERATOR(ROWCOUNT => 10000))
        WHERE DATEADD(DAY, seq4(), DATE ''${start_date_str}'') <= DATE ''${end_date_str}''
        ORDER BY date_key
    `;
    snowflake.createStatement({sqlText: insertSQL}).execute();

    /*-------------------------------------------------
      6. AUDIT SUCCESS
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
      7. AUDIT FAILED
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
