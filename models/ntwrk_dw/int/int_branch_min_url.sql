    select campaign        as show_id
      , M_title
      , min(url_length) as min_url_length
     from {{ ref('int_branch_urls')}}
    group by 1, 2
