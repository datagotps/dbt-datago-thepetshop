SELECT 
    customerid,
    STRING_AGG(emailid, ', ') AS emailids,
    STRING_AGG(FORMAT_DATE('%Y-%m-%d', DATE(insertedon)), ', ') AS inserted_dates,
    STRING_AGG(CAST(_fivetran_deleted AS STRING), ', ') AS deletion_flags,
    SUM(CASE WHEN _fivetran_deleted = TRUE THEN 1 ELSE 0 END) AS deletion_count
FROM  {{ source('mysql_ofs', 'customerdetails') }} 
WHERE customerid IN (
    SELECT customerid
    FROM {{ source('mysql_ofs', 'customerdetails') }} 
    GROUP BY customerid
    HAVING COUNT(DISTINCT _fivetran_deleted) > 1  -- Indicates deleted and active records exist for same customerid
)
GROUP BY customerid
ORDER BY deletion_count DESC, customerid