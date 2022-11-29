WITH cte1 AS (
    SELECT a, b FROM table1
),
cte2 AS (
    SELECT c, e AS d FROM table2
)

SELECT
    b, d AS dd
FROM cte1 AS t
JOIN cte2
WHERE
    cte1.a = cte2.c
