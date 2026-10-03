-- D2: MIN / MAX on common scalar types (per group).
SELECT
  region,
  MIN(quantity),
  MAX(quantity),
  MIN(line_total),
  MAX(line_total)
FROM order_lines
GROUP BY region;
