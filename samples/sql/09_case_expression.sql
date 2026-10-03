-- D1: CASE expression in the projection list.
SELECT
  region,
  CASE WHEN quantity < 5 THEN 1 ELSE quantity END AS bucket
FROM order_lines;
