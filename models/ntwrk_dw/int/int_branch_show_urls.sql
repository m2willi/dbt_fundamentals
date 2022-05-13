select a.show_id
     , a.m_title
     , b.url
     , ds.show_title
from {{ ref('int_branch_min_url')}}
    join {{ ref('int_branch_urls')}} b on a.show_id = b.campaign and a.m_title = b.m_title and a.min_url_length = url_length --attributing the shortest url to find the show_specific info
    join ntwrk_dw.d_show ds on ds.show_id = a.show_id --attributing the shortest url to find the show_specific info
group by 1, 2, 3, 4