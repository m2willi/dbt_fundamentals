SELECT id as show_id,
    row_number() OVER (order by dl_show.starttime) as total_show_number
FROM {{ ref('stg_dl_show')}}
LEFT JOIN {{ ref('int_show_was_visible')}} wv 
    on wv.show_id = dl_show.id
WHERE 
    dl_show.sponsored = 1 OR wv.was_visible = 1
