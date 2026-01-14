CREATE OR REPLACE PROCEDURE INSHORTS_DB.SYS_AUDIT.SP_LOAD_DAILY_CONTENT_REPORT("BATCH_ID" VARCHAR)
RETURNS VARCHAR
LANGUAGE JAVASCRIPT
EXECUTE AS CALLER
AS '
var procName = ''INSHORTS_DB.SYS_AUDIT.SP_LOAD_DAILY_CONTENT_REPORT'';
var tgtTable = ''INSHORTS_DB.REPORTING.DAILY_CONTENT_REPORT'';
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
            WITH content_activity AS (
                SELECT
                    f.content_id,
                    c.CATEGORIES,
                    f.event_date,
                    COUNT(*) AS total_reads,
                    SUM(f.timespent) AS total_time_spent,
                    SUM(
                        CASE 
                            WHEN s.user_segment = ''Power User'' THEN 1 
                            ELSE 0 
                        END
                    ) AS reads_by_power_users
                FROM INSHORTS_DB.INTEGRATION.INT_FACT_USER_EVENT f
                JOIN INSHORTS_DB.INTEGRATION.INT_DIM_CONTENT c
                  ON f.content_id = c.content_id
                LEFT JOIN INSHORTS_DB.ANALYTICS.USER_SEGMENT s
                  ON f.deviceid = s.deviceid
                GROUP BY f.content_id, c.CATEGORIES, f.event_date
            ),

            viral_content AS (
                SELECT
                    event_date,
                    content_id,
                    CATEGORIES,
                    total_reads,
                    total_time_spent,
                    reads_by_power_users,
                    NTILE(20) OVER (
                        PARTITION BY event_date 
                        ORDER BY total_reads DESC
                    ) AS top_5_percent_rank
                FROM content_activity
            ),

            category_reads AS (
                SELECT
                    event_date,
                    CATEGORIES,
                    SUM(total_reads) AS category_total_reads
                FROM content_activity
                GROUP BY event_date, CATEGORIES
            ),

            top_category_per_day AS (
                SELECT
                    event_date,
                    CATEGORIES AS top_category
                FROM (
                    SELECT *,
                           ROW_NUMBER() OVER (
                               PARTITION BY event_date 
                               ORDER BY category_total_reads DESC
                           ) AS rn
                    FROM category_reads
                )
                WHERE rn = 1
            )

            SELECT
                v.event_date,
                t.top_category,
                v.content_id,

                CASE
                    WHEN v.top_5_percent_rank = 1 THEN ''Viral''
                    WHEN v.reads_by_power_users >= 3 THEN ''Share-worthy''
                END AS content_type,

                v.total_reads,
                v.reads_by_power_users,
                v.total_time_spent,

                ''${BATCH_ID}''     AS batch_id,
                CURRENT_TIMESTAMP AS created_ts
            FROM viral_content v
            JOIN top_category_per_day t
              ON v.event_date = t.event_date
            WHERE v.top_5_percent_rank = 1
               OR v.reads_by_power_users >= 3
            ORDER BY v.event_date, content_type DESC, total_reads DESC
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
