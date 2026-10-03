-- D4: uncorrelated subqueries in WHERE (IN / EXISTS) on the demo dataset.
SELECT region, quantity
FROM order_lines
WHERE quantity IN (SELECT min_qty FROM rebate_tiers WHERE min_qty >= 6)
  AND EXISTS (SELECT min_qty FROM rebate_tiers);
