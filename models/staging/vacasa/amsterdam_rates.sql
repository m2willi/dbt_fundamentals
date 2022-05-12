select
  listing_id ,
  date ,
  available ,
  price ,
  adjusted_price ,
  minimum_nights ,
  maximum_nights 
from {{ source('property_scrape_amsterdam', 'RATES') }}
 