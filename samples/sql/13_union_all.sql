-- D3: UNION ALL stacks compatible SELECT results (no deduplication).
SELECT region FROM order_lines WHERE quantity > 10
UNION ALL
SELECT region FROM order_lines WHERE line_total < 100.0;
