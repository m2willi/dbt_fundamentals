     select last_attributed_touch_data__$marketing_title as m_title
          , "last_attributed_touch_data__~campaign"      as campaign
          , "last_attributed_touch_data__+url"           as url
          , len("last_attributed_touch_data__+url")      as url_length
     from {{ ref('src_branch_data')}}
     group by 1, 2, 3, 4
