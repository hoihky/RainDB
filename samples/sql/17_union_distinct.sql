-- D5: UNION (distinct) removes duplicate regions across branches.
SELECT region FROM order_lines WHERE quantity > 10
UNION
SELECT region FROM order_lines WHERE line_total < 100.0;
