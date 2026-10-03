-- D5: correlated NOT EXISTS — rows whose quantity is not an exact rebate tier minimum.
SELECT region, quantity
FROM order_lines
WHERE NOT EXISTS (
  SELECT 1 FROM rebate_tiers
  WHERE rebate_tiers.min_qty = order_lines.quantity
)
ORDER BY quantity;
