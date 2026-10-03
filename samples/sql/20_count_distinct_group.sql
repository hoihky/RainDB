-- D5: COUNT(DISTINCT) per group with ORDER BY.
SELECT region, COUNT(DISTINCT quantity)
FROM order_lines
GROUP BY region
ORDER BY region;
