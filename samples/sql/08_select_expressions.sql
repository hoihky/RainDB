-- D1: computed columns (Int32 arithmetic) in SELECT and WHERE.
SELECT region, quantity + 1 AS bumped_qty
FROM order_lines
WHERE quantity + 1 > 5;
