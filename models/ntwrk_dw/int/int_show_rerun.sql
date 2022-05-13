             select dl2.id                                                    as original_show_id
                 , dl1.id                                                    as rerun_show_id
                 , cast(dl2.id as varchar) || ':' || cast(dl1.id as varchar) as rerun_id
            from {{ref('stg_dl_show')}} dl1
                     join {{ref('stg_dl_show')}} dl2
                          on (dl1.streampath = dl2.recordpath OR dl1.uploadpath = dl2.recordpath)
                              and dl1.id != dl2.id
            where dl2.removedat is null                --date_trunc('day',show_start_datetime) between '2021-10-01' and '2022-03-03' and
              and dl1.removedat is null
              and dl1.streampath != ''
              and dl2.recordpath is not null
              and rerun_show_id not in (2546, 2493) ---removing these two shows since so many other shows use the same shitty lofi, they aren't reruns
            group by 1, 2, 3
