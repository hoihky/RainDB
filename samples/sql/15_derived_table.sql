-- D4: derived table in FROM with predicates on the outer query.
SELECT d.region, d.quantity
FROM (SELECT region, quantity FROM order_lines WHERE line_total > 100.0) d
WHERE d.quantity NOT IN (SELECT min_qty FROM rebate_tiers WHERE min_qty < 5)
ORDER BY d.quantity
LIMIT 5;
