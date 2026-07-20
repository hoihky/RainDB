-- Global aggregate (no GROUP BY in strict subset).
SELECT SUM(line_total)
FROM order_lines;
