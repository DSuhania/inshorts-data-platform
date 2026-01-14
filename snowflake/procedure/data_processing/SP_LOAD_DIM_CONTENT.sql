CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_DIM_CONTENT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_DIM_CONTENT'';
var tgtTable = ''INSHORTS_DB.INTEGRATION.INT_DIM_CONTENT'';
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
      3. MERGE (SCD2)
    -------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            MERGE INTO INSHORTS_DB.INTEGRATION.INT_DIM_CONTENT tgt
            USING (
                SELECT
                    _id            AS content_id,
                    createdAt      AS published_at,
                    newsLanguage,
                    categories,
                    author,
                    HASH(newsLanguage, categories, author) AS record_hash
                FROM INSHORTS_DB.RAW_LANDING.CONTENT_LANDING
            ) src
            ON tgt.content_id = src.content_id
           AND tgt.is_current = TRUE

            WHEN MATCHED
             AND tgt.record_hash <> src.record_hash
            THEN UPDATE SET
                is_current = FALSE,
                effective_end_dt = CURRENT_DATE,
                updated_ts = CURRENT_TIMESTAMP

            WHEN NOT MATCHED THEN
            INSERT (
                content_id,
                published_at,
                newsLanguage,
                categories,
                author,
                record_hash,
                effective_start_dt,
                effective_end_dt,
                is_current,
                batch_id,
                created_ts,
                updated_ts
            )
            VALUES (
                src.content_id,
                src.published_at,
                src.newsLanguage,
                src.categories,
                src.author,
                src.record_hash,
                CURRENT_DATE,
                ''9999-12-31'',
                TRUE,
                ''${BATCH_ID}'',
                CURRENT_TIMESTAMP,
                CURRENT_TIMESTAMP
            )
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
