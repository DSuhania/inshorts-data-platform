CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.PROC_MASTER_PROCESS("PROCESS_NAME" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var batchId;
try {

    /*--------------------------------------------------
      0. CLOSE ANY STALE RUNNING BATCHES (SELF-HEALING)
    --------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.PROCESS_BATCH_AUDIT
            SET STATUS = ''FAILED'',
                ERROR_MESSAGE = ''ABORTED / PREVIOUS RUN DID NOT COMPLETE'',
                PROCESS_END_TS = CURRENT_TIMESTAMP
            WHERE PROCESS_NAME = ''${PROCESS_NAME}''
              AND STATUS = ''RUNNING''
        `
    }).execute();

    /*--------------------------------------------------
      1. GET PROCESS_ID
    --------------------------------------------------*/
    var ps = snowflake.createStatement({
        sqlText: `
            SELECT PROCESS_ID
            FROM INSHORTS_DB.SYS_AUDIT.PROCESS_MASTER
            WHERE PROCESS_NAME = ''${PROCESS_NAME}''
        `
    }).execute();

    if (!ps.next()) {
        throw `Invalid PROCESS_NAME: ${PROCESS_NAME}`;
    }

    var processId = ps.getColumnValue(1);

    /*--------------------------------------------------
      2. GENERATE BATCH_ID
    --------------------------------------------------*/
    var rs = snowflake.createStatement({
        sqlText: `SELECT INSHORTS_DB.SYS_AUDIT.BATCH_ID_SEQ.NEXTVAL`
    }).execute();

    rs.next();
    batchId = rs.getColumnValue(1);

    /*--------------------------------------------------
      3. INSERT RUNNING RECORD
    --------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            INSERT INTO INSHORTS_DB.SYS_AUDIT.PROCESS_BATCH_AUDIT
            (PROCESS_ID, PROCESS_NAME, BATCH_ID, PROCESS_START_TS,
             PROCESS_END_TS, STATUS, ERROR_MESSAGE)
            VALUES
            (${processId}, ''${PROCESS_NAME}'', ${batchId},
             CURRENT_TIMESTAMP, NULL, ''RUNNING'', NULL)
        `
    }).execute();

    /*--------------------------------------------------
      4. EXECUTE CHILD PROCEDURES SEQUENTIALLY
    --------------------------------------------------*/
    var seqRs = snowflake.createStatement({
        sqlText: `
            SELECT DISTINCT SEQ_NO
            FROM INSHORTS_DB.SYS_AUDIT.PROCESS_SEQ
            WHERE PROCESS_ID = ${processId}
            ORDER BY SEQ_NO
        `
    }).execute();

    while (seqRs.next()) {

        var seq = seqRs.getColumnValue(1);

        var procRs = snowflake.createStatement({
            sqlText: `
                SELECT PROCEDURE_NAME, PROCEDURE_ARGS
                FROM INSHORTS_DB.SYS_AUDIT.PROCESS_SEQ
                WHERE PROCESS_ID = ${processId}
                  AND SEQ_NO = ${seq}
            `
        }).execute();

        while (procRs.next()) {

            var pName = procRs.getColumnValue(1);
            var args  = procRs.getColumnValue(2);

            var callSql = `CALL ${pName}(${args}, ${batchId})`;

            try {
                snowflake.createStatement({ sqlText: callSql }).execute();
            } catch (procErr) {

                snowflake.createStatement({
                    sqlText: `
                        UPDATE INSHORTS_DB.SYS_AUDIT.PROCESS_BATCH_AUDIT
                        SET STATUS = ''FAILED'',
                            ERROR_MESSAGE = ''FAILED IN ${pName}: ${procErr.toString().replace("''", "")}'',
                            PROCESS_END_TS = CURRENT_TIMESTAMP
                        WHERE BATCH_ID = ${batchId}
                    `
                }).execute();

                throw procErr;
            }
        }
    }

    /*--------------------------------------------------
      5. MARK SUCCESS
    --------------------------------------------------*/
    snowflake.createStatement({
        sqlText: `
            UPDATE INSHORTS_DB.SYS_AUDIT.PROCESS_BATCH_AUDIT
            SET STATUS = ''SUCCESS'',
                PROCESS_END_TS = CURRENT_TIMESTAMP
            WHERE BATCH_ID = ${batchId}
        `
    }).execute();

    return ''PROCESS COMPLETED SUCCESSFULLY'';

} catch (err) {

    if (batchId !== undefined) {
        snowflake.createStatement({
            sqlText: `
                UPDATE INSHORTS_DB.SYS_AUDIT.PROCESS_BATCH_AUDIT
                SET STATUS = ''FAILED'',
                    ERROR_MESSAGE = ''${err.toString().replace("''", "")}'',
                    PROCESS_END_TS = CURRENT_TIMESTAMP
                WHERE BATCH_ID = ${batchId}
            `
        }).execute();
    }

    throw err;
}
';
