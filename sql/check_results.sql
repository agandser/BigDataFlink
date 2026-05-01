SELECT 'fact_sales' AS table_name, COUNT(*) AS row_count FROM fact_sales
UNION ALL
SELECT 'dim_customer', COUNT(*) FROM dim_customer
UNION ALL
SELECT 'dim_seller', COUNT(*) FROM dim_seller
UNION ALL
SELECT 'dim_product', COUNT(*) FROM dim_product
UNION ALL
SELECT 'dim_store', COUNT(*) FROM dim_store
UNION ALL
SELECT 'dim_supplier', COUNT(*) FROM dim_supplier
ORDER BY table_name;

SELECT source_file, COUNT(*) AS sales_count
FROM fact_sales
GROUP BY source_file
ORDER BY source_file;

SELECT
    c.country,
    ROUND(SUM(f.total_price), 2) AS revenue,
    COUNT(*) AS sales_count
FROM fact_sales f
JOIN dim_customer c ON c.customer_id = f.customer_id
GROUP BY c.country
ORDER BY revenue DESC
LIMIT 10;

SELECT
    p.category,
    ROUND(AVG(p.rating), 2) AS avg_rating,
    COUNT(*) AS sales_count,
    ROUND(SUM(f.total_price), 2) AS revenue
FROM fact_sales f
JOIN dim_product p ON p.product_id = f.product_id
GROUP BY p.category
ORDER BY sales_count DESC;

SELECT
    DATE_TRUNC('month', f.sale_date)::date AS sale_month,
    COUNT(*) AS sales_count,
    ROUND(SUM(f.total_price), 2) AS revenue
FROM fact_sales f
GROUP BY sale_month
ORDER BY sale_month;
