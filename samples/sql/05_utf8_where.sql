-- UTF-8 column predicate (= / != with a single-quoted literal).
SELECT region, quantity
FROM order_lines
WHERE region = 'US-East';
