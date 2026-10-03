-- D5: correlated EXISTS on the analytics demo (per-row nested scan).
SELECT region, quantity
FROM order_lines
WHERE EXISTS (
  SELECT 1 FROM rebate_tiers
  WHERE rebate_tiers.min_qty = order_lines.quantity
)
ORDER BY quantity;
