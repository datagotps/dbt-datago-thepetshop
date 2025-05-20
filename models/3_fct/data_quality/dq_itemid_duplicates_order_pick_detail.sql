-- Find itemid values that are causing duplicates in the join
WITH duplicates AS (
  SELECT 
    a.itemid,
    COUNT(b.itemid) AS match_count
  FROM `tps-data-386515`.`dbt_dev_datago_int`.`int_occ_order_items` AS a
  JOIN `tps-data-386515`.`dbt_dev_datago_stg`.`stg_petshop_pick_detail` AS b
    ON b.itemid = a.itemid
  GROUP BY a.itemid
  HAVING COUNT(b.itemid) > 1
)

-- Display the duplicate records
SELECT 
  a.*,
  b.*
FROM `tps-data-386515`.`dbt_dev_datago_int`.`int_occ_order_items` AS a
JOIN duplicates d
  ON a.itemid = d.itemid
JOIN `tps-data-386515`.`dbt_dev_datago_stg`.`stg_petshop_pick_detail` AS b
  ON b.itemid = a.itemid
ORDER BY a.itemid