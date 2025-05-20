-- Find the itemid values with multiple matches in petshop_pna_details
WITH duplicate_items AS (
  SELECT 
    a.itemid,
    COUNT(b.item_id) AS match_count
  FROM `tps-data-386515`.`dbt_dev_datago_int`.`int_occ_order_items` AS a
  JOIN `tps-data-386515`.`sql_erp_prod_dbo`.`petshop_pna_details_c91094c2_db03_49d2_8cb9_95c179ecbf9d` AS b 
    ON a.itemid = b.item_id
  GROUP BY a.itemid
  HAVING COUNT(b.item_id) > 1
)

-- Show the duplicate records
SELECT 
  a.itemid,
  di.match_count AS number_of_matches,
  --b.item_id,
  b.* -- Include all fields from pna_details
FROM `tps-data-386515`.`dbt_dev_datago_int`.`int_occ_order_items` AS a
JOIN duplicate_items di
  ON a.itemid = di.itemid
JOIN `tps-data-386515`.`sql_erp_prod_dbo`.`petshop_pna_details_c91094c2_db03_49d2_8cb9_95c179ecbf9d` AS b
  ON a.itemid = b.item_id
ORDER BY a.itemid, b.item_id