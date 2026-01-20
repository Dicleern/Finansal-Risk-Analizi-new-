SELECT
      CAST(zip_code AS INT64) as zip_code
    , city
    , state_code as state
     /* Since the geometry data was imported as text from the CSV, I parsed it back 
     into geographical objects. I then calculated the centroids to extract 
     the specific latitude and longitude coordinates needed for my distance analysis. */
    , ST_Y(ST_CENTROID(ST_GEOGFROMTEXT(zip_code_geom))) as latitude
    , ST_X(ST_CENTROID(ST_GEOGFROMTEXT(zip_code_geom))) as longitude
FROM {{ source('finansal_analiz', 'zip_code_public') }}
