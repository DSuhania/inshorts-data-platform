CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.PROC_RUN_DQ("TARGET_TABLE" VARCHAR, "BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
try {

    // 1. Fetch active DQ rules for the table
    var ruleSql = `
        SELECT RULE_ID, TARGET_COLUMN, CHECK_TYPE, RULE_EXPRESSION, THRESHOLD
        FROM INSHORTS_DB.SYS_AUDIT.DQ_RULES
        WHERE TARGET_TABLE = ''${TARGET_TABLE}'' AND IS_ACTIVE = TRUE
    `;
    var ruleRs = snowflake.createStatement({ sqlText: ruleSql }).execute();

    if (!ruleRs.next()) {
        throw new Error(''No active DQ rules found for '' + TARGET_TABLE);
    }

    do {
        var ruleId = ruleRs.getColumnValue(1);
        var column = ruleRs.getColumnValue(2);
        var checkType = ruleRs.getColumnValue(3);
        var expr = ruleRs.getColumnValue(4);
        var threshold = ruleRs.getColumnValue(5);

        var checkSql = '''';
        if (checkType == ''COMPLETENESS'') {
            checkSql = `
                SELECT 
                    COUNT(*) AS ROWS_CHECKED,
                    COUNT(CASE WHEN ${column} IS NULL THEN 1 END) AS ROWS_FAILED
                FROM ${TARGET_TABLE}
                WHERE BATCH_ID = ''${BATCH_ID}''
            `;
        } else if (checkType == ''UNIQUE'') {
            checkSql = `
                SELECT 
                    COUNT(*) AS ROWS_CHECKED,
                    COUNT(*) - COUNT(DISTINCT ${column}) AS ROWS_FAILED
                FROM ${TARGET_TABLE}
                WHERE BATCH_ID = ''${BATCH_ID}''
            `;
        } else {
            // For RANGE, PATTERN, or custom SQL
            checkSql = `
                SELECT 
                    COUNT(*) AS ROWS_CHECKED,
                    COUNT(*) - COUNT(CASE WHEN ${expr} THEN 1 END) AS ROWS_FAILED
                FROM ${TARGET_TABLE}
                WHERE BATCH_ID = ''${BATCH_ID}''
            `;
        }

        var checkRsExec = snowflake.createStatement({ sqlText: checkSql }).execute();
        var rowsChecked = 0;
        var rowsFailed = 0;
        if (checkRsExec.next()) {
            rowsChecked = checkRsExec.getColumnValue(1);
            rowsFailed = checkRsExec.getColumnValue(2);
        }

        var status = (rowsFailed == 0) ? ''PASS'' : ''FAIL'';

        // Insert into DQ Audit
        var auditSql = `
            INSERT INTO INSHORTS_DB.SYS_AUDIT.DQ_AUDIT
            (BATCH_ID, TARGET_TABLE, TARGET_COLUMN, CHECK_NAME, CHECK_TYPE, STATUS,
             ROWS_CHECKED, ROWS_FAILED, ACTUAL_VALUE, EXPECTED_VALUE, ERROR_MESSAGE)
            VALUES
            (''${BATCH_ID}'', ''${TARGET_TABLE}'', ''${column}'', ''RULE_${ruleId}'', ''${checkType}'',
             ''${status}'', ${rowsChecked}, ${rowsFailed}, ''${rowsFailed}'', ''${threshold}'', NULL)
        `;
        snowflake.createStatement({ sqlText: auditSql }).execute();

    } while (ruleRs.next());

    return ''DQ_CHECKS_COMPLETED'';

} catch (err) {

    var msg = err.message ? err.message.replace(/''/g, '''') : ''UNKNOWN_ERROR'';

    // Log failure in audit table
    var failAuditSql = `
        INSERT INTO INSHORTS_DB.SYS_AUDIT.DQ_AUDIT
        (BATCH_ID, TARGET_TABLE, CHECK_NAME, CHECK_TYPE, STATUS, ERROR_MESSAGE, CHECK_TS)
        VALUES
        (''${BATCH_ID}'', ''${TARGET_TABLE}'', ''DQ_PROC_FAILURE'', ''SYSTEM'', ''FAILED'', ''${msg}'', CURRENT_TIMESTAMP)
    `;
    snowflake.createStatement({ sqlText: failAuditSql }).execute();

    throw err;
}
';
