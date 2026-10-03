-- D3: LEFT JOIN keeps every left row; unmatched right columns are NULL.
SELECT order_lines.region, rebate_tiers.rebate_pct
FROM order_lines
LEFT JOIN rebate_tiers ON order_lines.quantity = rebate_tiers.min_qty;
