-- D5: FULL OUTER JOIN returns unmatched rows from either side (null-padded).
SELECT order_lines.region, rebate_tiers.rebate_pct
FROM order_lines
FULL OUTER JOIN rebate_tiers ON order_lines.quantity = rebate_tiers.min_qty;
