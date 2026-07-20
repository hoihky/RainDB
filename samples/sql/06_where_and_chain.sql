-- AND conjunction of predicates on one table.
SELECT region, line_total
FROM order_lines
WHERE quantity >= 6 AND line_total < 5000;
