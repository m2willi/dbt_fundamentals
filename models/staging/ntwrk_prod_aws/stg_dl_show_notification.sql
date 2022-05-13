select
    _sdc_batched_at ,  
    _sdc_extracted_at ,
    _sdc_received_at  ,
    _sdc_sequence     ,
    _sdc_table_version,
    customerid        ,
    id                ,
    removedat         ,
    showid            ,
    createdat         ,
    updatedat          
from {{ source('ntwrk_prod_aws', 'dl_show_notification') }}