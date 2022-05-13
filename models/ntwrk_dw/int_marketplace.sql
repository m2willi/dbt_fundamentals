SELECT t1.show_id,
        CASE WHEN marketplace_products = 0 THEN 1
        WHEN marketplace_products = show_products THEN 2
        WHEN marketplace_products < show_products THEN 3
            else null end as show_marketplace_id,
        CASE WHEN marketplace_products = 0 THEN 'ntwrk'
        WHEN marketplace_products = show_products THEN 'marketplace'
        WHEN marketplace_products < show_products THEN 'partial marketplace'
            else null end as show_marketplace_type
FROM (
        SELECT sp.show_id,
            sum(marketplace_product) as marketplace_products,
            count(sp.show_product_id) as show_products
        FROM ntwrk_dw.f_show_product sp
        group by 1
    ) t1