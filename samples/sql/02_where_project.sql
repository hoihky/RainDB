-- Projection + filter (UTF-8 columns may appear in SELECT; WHERE must be fixed-width).
SELECT region, line_total
FROM order_lines
WHERE quantity >= 6;
