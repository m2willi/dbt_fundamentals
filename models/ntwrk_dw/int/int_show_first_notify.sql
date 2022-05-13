SELECT 
    sn.showid, 
    min(sn.createdat) as first_notify
FROM 
    {{ ref('stg_dl_show_notification')}} sn
LEFT JOIN {{ ref('stg_dl_show')}} sh 
    on sh.id = sn.showid
WHERE 
    sn.createdat < sh.starttime
