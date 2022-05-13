select
last_attributed_touch_data__$marketing_title  
          , "last_attributed_touch_data__~campaign"     
          , "last_attributed_touch_data__+url"           
          , len("last_attributed_touch_data__+url")      
from {{ source('branch', 'data') }}