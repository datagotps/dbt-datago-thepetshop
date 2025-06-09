{{ config(
    materialized = 'table',
    description = 'ISO-standard calendar date dimension with one row per day and standard time attributes, including required sorting fields for Power BI.'
) }}

WITH params AS (
  SELECT 
    DATE '2021-01-01' AS start_date,
    DATE '2040-12-31' AS end_date
),

-- Generate daily dates between start and end date
raw_dates AS (
  SELECT
    start_date + INTERVAL n DAY AS date
  FROM params,
       UNNEST(GENERATE_ARRAY(0, DATE_DIFF(end_date, start_date, DAY))) AS n
),

calendar AS (
  SELECT
    date,  -- Date (e.g., 2025-06-01)

    EXTRACT(YEAR FROM date) AS year,  -- Year (e.g., 2025)
    EXTRACT(MONTH FROM date) AS month_number,  -- Month number (e.g., 6 for June)
    FORMAT_DATE('%B', date) AS month_name,  -- Full month name (e.g., June)
    FORMAT_DATE('%b', date) AS month_abbr,  -- Abbreviated month name (e.g., Jun)
    EXTRACT(DAY FROM date) AS day_of_month,  -- Day of the month (e.g., 15)

    MOD(EXTRACT(DAYOFWEEK FROM date) + 5, 7) + 1 AS day_of_week_number,  -- Monday = 1, Sunday = 7
    FORMAT_DATE('%A', date) AS day_of_week_name,  -- Full day name (e.g., Monday)
    FORMAT_DATE('%a', date) AS day_of_week_abbr,  -- Abbreviated day name (e.g., Mon)

    EXTRACT(ISOWEEK FROM date) AS iso_week_number,  -- ISO week number (1–53)
    EXTRACT(ISOYEAR FROM date) AS iso_year,  -- ISO week-based year
    FORMAT_DATE('%G-W%V', date) AS iso_year_week,  -- ISO year-week label (e.g., 2025-W23)
    SAFE_CAST(FORMAT_DATE('%G%V', date) AS INT64) AS iso_year_week_sort,  -- Sortable ISO week (e.g., 202523)

    FORMAT_DATE('%Y-%m', date) AS year_month,  -- Year-Month label (e.g., 2025-06)
    FORMAT_DATE('%b %Y', date) AS month_year_label,  -- e.g., Jan 2025
    SAFE_CAST(FORMAT_DATE('%Y%m', date) AS INT64) AS year_month_sort,  -- Sortable Year-Month (e.g., 202506)

    EXTRACT(QUARTER FROM date) AS quarter_number,  -- Quarter number (1–4)
    CONCAT('Q', CAST(EXTRACT(QUARTER FROM date) AS STRING)) AS quarter,  -- Quarter label (e.g., Q2)
    FORMAT_DATE('%Y-Q%Q', date) AS year_quarter,  -- Year-Quarter label (e.g., 2025-Q2)
    SAFE_CAST(FORMAT_DATE('%Y%Q', date) AS INT64) AS year_quarter_sort,  -- Sortable Year-Quarter (e.g., 20252)

    CASE WHEN MOD(EXTRACT(DAYOFWEEK FROM date) + 5, 7) + 1 IN (6, 7) THEN TRUE ELSE FALSE END AS is_weekend,  -- TRUE for Saturday or Sunday
    CASE WHEN date = CURRENT_DATE() THEN TRUE ELSE FALSE END AS is_today,  -- TRUE if today
    CASE 
      WHEN DATE_TRUNC(DATE_ADD(date, INTERVAL 1 DAY), MONTH) != DATE_TRUNC(date, MONTH)
      THEN TRUE ELSE FALSE
    END AS is_last_day_of_month,  -- TRUE if the last day of the month

    CASE 
      WHEN date = CURRENT_DATE() THEN 'Today'
      WHEN date < CURRENT_DATE() THEN FORMAT_DATE('%b %d, %Y', date)
      ELSE NULL
    END AS slicer_label  -- User-friendly label for slicers (e.g., "Jun 05, 2025", "Today")

  FROM raw_dates
)

SELECT * FROM calendar
ORDER BY date
