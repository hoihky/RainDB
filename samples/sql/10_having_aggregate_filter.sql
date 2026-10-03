-- D2: HAVING filters groups after aggregation (not individual rows).
SELECT region, SUM(line_total)
FROM order_lines
GROUP BY region
HAVING SUM(line_total) > 500.0;
