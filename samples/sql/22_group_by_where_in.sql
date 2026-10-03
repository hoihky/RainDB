-- GROUP BY with uncorrelated IN in WHERE (filters rows before aggregation).
SELECT region, SUM(line_total)
FROM order_lines
WHERE quantity IN (SELECT min_qty FROM rebate_tiers)
GROUP BY region
ORDER BY region;
