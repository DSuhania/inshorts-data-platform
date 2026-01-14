CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.PROC_INGEST_TARGET("TARGET_TABLE" VARCHAR, "BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {

    /*--------------------------------------------------
      1. Read ingestion metadata
    --------------------------------------------------*/
    var metaSql = `
        SELECT INGESTION_ID,
               SOURCE_STAGE,
               FILE_PATTERN,
               FILE_DELIMITER,
               FILE_EXTENSION
        FROM INSHORTS_DB.SYS_AUDIT.INGESTION_OBJECT_MAPPING
        WHERE TARGET_TABLE_NAME = ''${TARGET_TABLE}''
    `;
    var metaRs = snowflake.createStatement({ sqlText: metaSql }).execute();

    if (!metaRs.next()) {
        throw new Error(''No ingestion config found for '' + TARGET_TABLE);
    }

    var ingestionId = metaRs.getColumnValue(1);
    var stage       = metaRs.getColumnValue(2);
    var pattern     = metaRs.getColumnValue(3);
    var delimiter   = metaRs.getColumnValue(4);
    var extension   = metaRs.getColumnValue(5);

    /*--------------------------------------------------
      2. Build TARGET column list + SELECT expressions
    --------------------------------------------------*/
    var colSql = `
        SELECT TARGET_COLUMN_NAME,
               TARGET_COLUMN_DATATYPE
        FROM INSHORTS_DB.SYS_AUDIT.INGESTION_COLUMN_MAPPING
        WHERE INGESTION_ID = ${ingestionId}
        ORDER BY SOURCE_COLUMN_SEQ
    `;
    var colRs = snowflake.createStatement({ sqlText: colSql }).execute();

    var targetCols = [];
    var selectCols = [];
    var pos = 1;

    while (colRs.next()) {
        targetCols.push(colRs.getColumnValue(1));
        selectCols.push(
            "TRY_CAST($" + pos + " AS " + colRs.getColumnValue(2) + ")"
        );
        pos++;
    }

    if (targetCols.length === 0) {
        throw new Error(''No column mappings found'');
    }

    /*--------------------------------------------------
      3. Append system columns
    --------------------------------------------------*/
    targetCols.push(''BATCH_ID'');
    targetCols.push(''PROCESSED_DATE'');

    selectCols.push("''" + BATCH_ID + "''");
    selectCols.push("CURRENT_TIMESTAMP");

    /*--------------------------------------------------
      4. COPY INTO with DYNAMIC COLUMN LIST
    --------------------------------------------------*/
    var copySql = `
        COPY INTO ${TARGET_TABLE} (
            ${targetCols.join('','')}
        )
        FROM (
            SELECT ${selectCols.join('','')}
            FROM ${stage}
        )
        FILE_FORMAT = (
            TYPE = ''${extension}''
            FIELD_DELIMITER = ''${delimiter}''
            SKIP_HEADER = 1
        )
        PATTERN = ''${pattern}''
        ON_ERROR = ''CONTINUE''
    `;
    var truncateSql = `TRUNCATE TABLE ${TARGET_TABLE}`;
    snowflake.createStatement({ sqlText: truncateSql }).execute();

    snowflake.createStatement({ sqlText: copySql }).execute();

    /*--------------------------------------------------
      5. Count rows for this batch
    --------------------------------------------------*/
    var countSql = `
        SELECT COUNT(*)
        FROM ${TARGET_TABLE}
        WHERE BATCH_ID = ''${BATCH_ID}''
    `;
    var countRs = snowflake.createStatement({ sqlText: countSql }).execute();

    var rowsInserted = 0;
    if (countRs.next()) {
        rowsInserted = countRs.getColumnValue(1);
    }

    if (rowsInserted === 0) {
        throw new Error(''No rows loaded for batch '' + BATCH_ID);
    }

    /*--------------------------------------------------
      6. Audit SUCCESS
    --------------------------------------------------*/
    var successAuditSql = `
        INSERT INTO INSHORTS_DB.SYS_AUDIT.INGESTION_AUDIT
        (INGESTION_ID, TARGET_TABLE_NAME, STATUS, ERROR_MESSAGE,
         ROWS_INSERTED, BATCH_ID, INSERT_DATE)
        VALUES
        (${ingestionId}, ''${TARGET_TABLE}'', ''SUCCESS'',
         NULL, ${rowsInserted}, ''${BATCH_ID}'', CURRENT_TIMESTAMP)
    `;
    snowflake.createStatement({ sqlText: successAuditSql }).execute();

    return ''SUCCESS'';

} catch (err) {

    var msg = err.message ? err.message.replace(/''/g, '''') : ''UNKNOWN_ERROR'';

    var failAuditSql = `
        INSERT INTO INSHORTS_DB.SYS_AUDIT.INGESTION_AUDIT
        (INGESTION_ID, TARGET_TABLE_NAME, STATUS, ERROR_MESSAGE,
         ROWS_INSERTED, BATCH_ID, INSERT_DATE)
        VALUES
        (NULL, ''${TARGET_TABLE}'', ''FAILED'',
         ''${msg}'', 0, ''${BATCH_ID}'', CURRENT_TIMESTAMP)
    `;
    snowflake.createStatement({ sqlText: failAuditSql }).execute();

    throw err;
}
';
