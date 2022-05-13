                SELECT sh.id as show_id
                FROM ntwrk_prod_aws.dl_show sh
                         LEFT JOIN ntwrk_dw.f_show_product sp on sp.show_id = sh.id
                         LEFT JOIN ntwrk_dw.d_product p on p.product_id = sp.product_id
                WHERE p.promo_drawing_product = 1
             GROUP BY 1
