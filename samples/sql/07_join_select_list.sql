-- Explicit SELECT list on a join (qualified columns). Join keys must be fixed-width types.
SELECT order_lines.region, rebate_tiers.rebate_pct
FROM order_lines
INNER JOIN rebate_tiers ON order_lines.quantity = rebate_tiers.min_qty
WHERE order_lines.quantity >= 6;
