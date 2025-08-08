select
*,

DATETIME_ADD(CURRENT_DATETIME(), INTERVAL 4 HOUR) AS report_last_updated_at, 

FROM {{ ref('int_commercial') }}