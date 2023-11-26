
\c - postgres
create extension if not exists pg_stat_statements;

-- which operations are the most time-consuming

SELECT substring(query, 1, 80) AS query, calls,
    round(total_exec_time::numeric, 2) AS total_time,
    round(mean_exec_time::numeric, 2) AS mean_time,
    round((100 * total_exec_time / sum(total_exec_time) OVER ())::numeric, 2) AS percentage
FROM  pg_stat_statements
ORDER BY total_exec_time DESC
LIMIT 10;

-- ALTER DATABASE test SET track_io_timing = on;

SELECT substring(query, 1, 80),
    round(total_exec_time::numeric, 2) AS total,
    round(blk_read_time::numeric, 2) AS read,
    round(blk_write_time::numeric, 2) AS write
FROM  pg_stat_statements
ORDER BY blk_read_time + blk_write_time DESC
LIMIT 10;
