     SELECT sh.id AS show_id,
            wv.was_visible,
    sh.ordering AS show_ordering,
  convert_timezone('America/Los_Angeles', sh.starttime) AS show_start_datetime,
       TRIM(sh.title)  AS show_title,
       CASE WHEN sp.product_count IS NULL
        THEN 0
          ELSE sp.product_count END                                                                   AS product_count,
       season.season,
       season.episode_number,
       season.overall_episode_number,
        season.season||case when season.episode_number < 10
                        then '00'||CAST(season.episode_number AS TEXT)
                            when season.episode_number < 100
                                then '0'||CAST(season.episode_number AS TEXT)
                                   else CAST(season.episode_number AS TEXT) END                                          AS season_episode,
       sh.showdatetype as show_date_type,
       CASE WHEN sh.showdatetype = 0
          THEN 'full_date'
        WHEN sh.showdatetype = 1
          THEN 'month'
        WHEN sh.showdatetype = 2
          THEN 'hidden_date'
            ELSE NULL
END as show_date_type_name
       ,
       CASE WHEN
           sp.show_type = 'drop' THEN 1
           WHEN sp.show_type = 'drawing' THEN 2
           WHEN sp.show_type = 'hybrid' THEN 3
           ELSE sh.showtype END  AS show_type_id,
       CASE WHEN sp.show_type = 'unknown' or sp.show_type IS NULL THEN
            CASE WHEN sh.showtype = 1 THEN 'drop'
                WHEN sh.showtype = 2 THEN 'drawing'
                    ELSE 'drop' END
                        ELSE sp.show_type END AS show_type,
         sh.secondaryshowtype as secondary_show_type_id,
         CASE WHEN sh.secondaryshowtype = 0
           THEN 'content'
           WHEN sh.secondaryshowtype = 1
             THEN 'flash'
             ELSE 'other' END as secondary_show_type,
         sh.combinedshipmethodid as combined_shipmethod_id,
         sh.combinedshippingpolicy as combined_shipping_policy,
         sh.showtilecolors as show_tile_color_id,
         CASE WHEN sh.showtilecolors = 0
          THEN 'black'
          WHEN sh.showtilecolors = 1
            THEN 'white'
              ELSE 'other' END AS show_tile_color,
         sh.viewenabled as view_enabled,
         sh.showstate as show_state,
       CASE WHEN sp.show_type = 'drawing' THEN 'drawing'
           WHEN sp.first_sale_drop_sum = sp.drop_product_count THEN 'new'
            WHEN sp.first_sale_drop_sum = 0 THEN 'old'
            WHEN sp.first_sale_drop_sum > 0 and sp.first_sale_drop_sum < sp.drop_product_count THEN 'combo'
                ELSE 'unknown' end as show_product_sale_type,
            CASE WHEN sh.starttime < getdate() THEN coalesce(m.show_marketplace_id, 1)
                ELSE m.show_marketplace_id END as show_marketplace_id,
            CASE WHEN sh.starttime < getdate() THEN coalesce(m.show_marketplace_type, 'ntwrk')
                ELSE m.show_marketplace_type END as show_marketplace_type,
                    CASE
                  WHEN pro.show_id IS NOT NULL
                      THEN 1
                  ELSE 0 END                                                                                 as promo_drawing_show,
            sf.festival as festival_name,
            CASE WHEN sf.festival is not null THEN 1 ELSE 0 END as is_festival,
            sh.sponsored                                                                                     as is_sponsored,
            num.total_show_number,
            case when sr.show_id is not null then 1 else 0 end as creator_created,
            sh.isapplive as is_app_live,
            convert_timezone('America/Los_Angeles', COALESCE(sh.activated_at, fn.first_notify)) as activated_at,
            convert_timezone('America/Los_Angeles', sh.createdat) as created_at,
            convert_timezone('America/Los_Angeles', sh.updatedat) as updated_at,
            convert_timezone('America/Los_Angeles', sh.removedat) as removed_at,
            teaserlengthseconds as teaser_length_seconds,
            vodlengthseconds as vod_length_seconds,
            CASE WHEN rs.rerun_show_id IS NOT NULL THEN 1 ELSE 0 END as is_rerun,
            rs.original_show_id as original_show_id,
            rs.rerun_id as rerun_id
FROM ntwrk_prod_aws.dl_show sh
    LEFT JOIN first_notify fn on fn.showid = sh.id
LEFT JOIN (
    SELECT t1.show_id,
    t1.show_quarter_number,
    t1.show_year,
    t1.show_quarter,
    t1.show_month,
    row_number() OVER (PARTITION BY show_month ORDER BY starttime) episode_number,
    CAST(date_part('year', convert_timezone('America/Los_Angeles', t1.starttime))-2000 AS TEXT)||CASE WHEN date_part('month', convert_timezone('America/Los_Angeles', t1.starttime)) < 10
        THEN '0'||CAST(date_part('month', convert_timezone('America/Los_Angeles', t1.starttime)) AS TEXT)
        ELSE CAST(date_part('month', convert_timezone('America/Los_Angeles', t1.starttime)) AS TEXT) END season,
    row_number() OVER (ORDER BY starttime) AS overall_episode_number
    FROM (
    SELECT sh.id as show_id,
    convert_timezone('America/Los_Angeles', starttime) AS starttime,
    extract (QUARTER FROM convert_timezone('America/Los_Angeles', starttime)) AS show_quarter_number,
    extract (YEAR FROM convert_timezone('America/Los_Angeles', starttime)) AS show_year,
    date_trunc('quarter', convert_timezone('America/Los_Angeles', starttime)) AS show_quarter,
    date_trunc('month', convert_timezone('America/Los_Angeles', starttime)) AS show_month
    FROM {{ref('stg_dl_show')}} sh
    LEFT JOIN {{ref('int_show_was_visible')}} wv on wv.show_id = sh.id
        WHERE was_visible = 1
    ) t1
    ORDER BY show_quarter
    ) season ON season.show_id = sh.id
LEFT JOIN (
    SELECT *, CASE WHEN t1.drawing_product_count = 0 and t1.drop_product_count = 0 then 'unknown'
            WHEN t1.drawing_product_count = 0 and t1.drop_product_count > 0 then 'drop'
            WHEN t1.drawing_product_count > 0 and t1.drop_product_count = 0 then 'drawing'
            WHEN t1.drawing_product_count > 0 and t1.drop_product_count > 0 then 'hybrid'
            WHEN t1.drawing_product_count IS NULL or t1.drop_product_count IS NULL THEN 'unknown'
                ELSE NULL END AS show_type
           FROM ( SELECT
                 sp.show_id as show_id,
                 count (distinct case when sp.sale_type_id = 1 then sp.product_id else null
                 end) as drop_product_count,
                    count (distinct case when sp.sale_type_id = 2 then sp.product_id else null end) as drawing_product_count,
                    count (distinct sp.product_id) as product_count,
                    sum(sp.first_sale) as first_sale_sum,
                    sum(CASE WHEN sp.sale_type_id = 1 then sp.first_sale ELSE 0 END) AS first_sale_drop_sum
                    FROM ntwrk_dw.f_show_product sp
               group by 1
                    ) t1
    ) sp on sp.show_id = sh.id
left join {{ref('int_marketplace')}} m on m.show_id = sh.id
left join {{ref('int_show_was_visible')}} wv on wv.show_id = sh.id
LEFT JOIN ntwrk_block.d_show_festival sf on sf.show_id = sh.id
    LEFT JOIN {{ref('int_show_numbering')}} num on num.show_id = sh.id
    LEFT JOIN promo pro on pro.show_id = sh.id
    LEFT JOIN (SELECT show_id
               FROM {{ref('stage show_requrest')}}
        GROUP BY 1
                    ) sr on sr.show_id = sh.id
    LEFT JOIN {{ref('int_show_rerun')}} rs on rs.rerun_show_id = sh.id
    WHERE sh.id <> 1473
ORDER BY 4
