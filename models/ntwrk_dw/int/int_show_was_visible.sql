       SELECT t1.show_id,
               CASE WHEN t1.removedat is not null THEN 0
                    WHEN title ilike '% test %' THEN 0
                   WHEN t1.showstate = 1 AND t1.starttime > getdate() THEN 1
                   WHEN transactions is null and viewers < 30 then 0
                   WHEN transactions > 0 or viewers >= 30 then 1
                   WHEN t1.showstate = 0 and t1.starttime > getdate() THEN 0
                    ELSE 0 END as was_visible
        from (
                 SELECT sh.id             as show_id,
                        sh.showstate,
                        sh.removedat,
                        t.transactions,
                        sv.viewers,
                        sh.title,
                        sh.starttime
                 FROM {{ ref('stg_dl_show')}} sh
                          LEFT JOIN (select showid, count(id) as transactions
                              FROM {{ ref('stg_dl_transaction')}}
                              group by 1) t on t.showid = sh.id
                          LEFT JOIN (SELECT show_id, count(user_id) viewers
                      FROM ntwrk_dw.f_user_show_views
                          WHERE live_view = 1
                      group by 1) sv on sv.show_id = sh.id
                 GROUP BY 1, 2, 3, 4, 5, 6, 7
             ) T1
