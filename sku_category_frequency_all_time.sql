-- Q: What is the average time between orders of the same SKU, across all customers? Interested in all time.
-- Create join of all purchased SKUs by customer.
-- Categorize SKUs based on Miranda's logic.
-- Add time parting logic to be used downstream.
WITH customer_purchases AS(
  SELECT
  a.customer_id,
  a.order_id,
  a.trxn_date,
  LAG(a.trxn_date) OVER (PARTITION BY a.customer_id, b.sku ORDER BY a.trxn_date) lag_trxn_date,
  b.sku,
  CASE
    WHEN sku IN('19-001-470',	'19-001-357',	'19-001-355',	'19-001-439',	'19-001-494',	'19-001-302',	'29-001-937',	'19-001-301',	'19-001-356',	'19-002-317',	'19-001-300',	'19-001-370') THEN 'Mothership'
    WHEN sku IN('20-020-482',	'20-020-486',	'20-020-013',	'20-020-055',	'20-020-118',	'20-020-016',	'20-020-215',	'20-020-020',	'20-020-056',	'20-020-028',	'20-020-049',	'20-020-221',	'20-020-047',	'20-020-057',	'20-020-015',	'20-020-040',	'20-020-117',	'20-020-053',	'20-020-067',	'20-020-018',	'20-020-024',	'20-020-027',	'20-020-041',	'20-020-048',	'20-020-457',	'20-020-448',	'20-020-119',	'20-020-025',	'20-020-451',	'39-001-1576',	'20-020-022',	'20-020-449',	'23-025-230',	'23-025-232',	'29-001-970',	'29-001-972',	'23-025-234',	'23-025-226',	'23-025-228',	'23-025-227',	'23-025-211',	'23-025-231',	'23-025-229',	'23-034-240',	'23-034-238',	'23-034-239',	'80-020-221',	'23-025-236',	'23-025-237',	'20-020-191',	'29-020-1586',	'20-020-190') THEN 'Matte Lipstick'
    WHEN sku IN('19-001-1622',	'19-001-1624',	'20-075-658',	'20-075-641',	'19-001-1632',	'20-075-649',	'20-075-654',	'19-001-1623',	'20-075-652',	'20-075-643',	'20-075-657',	'20-075-645',	'20-075-644',	'20-075-650',	'20-075-648',	'19-001-1628',	'20-075-659',	'20-075-642',	'19-001-1625',	'20-075-655',	'20-075-651',	'20-075-653',	'19-001-1631',	'20-075-646',	'20-075-660',	'19-001-1629',	'19-001-1626',	'20-075-661',	'20-075-656',	'19-001-1630',	'19-001-1627',	'20-075-496',	'20-075-494',	'20-075-493',	'20-075-495',	'20-075-492',	'20-022-072',	'20-023-176',	'20-023-132',	'20-022-073',	'20-023-135',	'20-023-136',	'20-022-074',	'20-022-188',	'20-022-071',	'20-023-138',	'20-022-189') THEN 'Cream Lipstick'
    WHEN sku IN('30-051-206',	'30-051-205',	'30-051-201',	'30-051-202',	'30-051-209',	'30-051-203',	'30-051-210',	'30-051-204',	'30-051-207',	'30-051-208',	'30-051-214',	'30-051-212',	'30-051-219',	'30-051-215',	'30-051-218',	'30-051-211',	'30-051-221',	'30-051-213',	'30-051-223',	'30-051-224',	'30-051-222',	'30-051-216',	'30-051-217',	'30-051-227',	'30-051-225',	'30-051-226',	'30-051-220',	'30-051-232',	'30-051-228',	'30-051-230',	'30-051-229',	'30-051-234',	'30-051-236',	'30-051-233',	'30-051-235') THEN 'Concealer'
    WHEN sku IN('30-014-188',	'30-014-187',	'30-014-166',	'30-014-186',	'30-014-161',	'30-014-178',	'30-014-164',	'30-014-169',	'30-014-163',	'30-014-165',	'30-014-172',	'30-014-179',	'30-014-167',	'30-014-162',	'30-014-189',	'30-014-175',	'30-014-191',	'30-014-174',	'30-014-190',	'30-014-170',	'30-014-171',	'30-014-176',	'30-014-180',	'30-014-193',	'30-014-177',	'30-014-192',	'30-014-182',	'30-014-173',	'30-014-194',	'30-014-184',	'30-014-195',	'30-014-181',	'30-014-185',	'30-014-196',	'30-014-168') THEN 'Foundation'
    WHEN sku IN('15-057-100',	'19-001-1595',	'15-050-150',	'15-057-109',	'15-057-106',	'15-057-108',	'15-057-107',	'15-057-105',	'15-057-103',	'15-057-104',	'17-050-150',	'19-001-1560') THEN 'Mascara'
    WHEN sku IN('36-070-286',	'36-070-312') THEN 'Essence'
    WHEN sku IN('33-067-337',	'33-067-271',	'33-067-274',	'33-067-336',	'33-067-272',	'33-067-283',	'33-067-277',	'33-067-282',	'33-067-273',	'33-067-275',	'33-067-276',	'33-067-315') THEN 'Blush Single'
    WHEN sku IN('33-067-305',	'33-067-309',	'33-067-308',	'33-067-310',	'33-067-304',	'33-067-303',	'39-001-1609') THEN 'Blush Duo'
    ELSE 'Other'
  END sku_category,
  DATE_DIFF(a.trxn_date, LAG(a.trxn_date) OVER (PARTITION BY a.customer_id, b.sku ORDER BY a.trxn_date), DAY) days_diff
  FROM `pmg-service-project-945896cb.dbt_prod.agg_customer` a
  JOIN `pmg-service-project-945896cb.dbt_prod.agg_customer_merchandise` b
  ON a.order_id = b.trxn_id
  WHERE a.trxn_type = 'order'
  --AND a.customer_id IN ('380629254168') -- Plug in Customer IDs for QA
)

-- Final query.
SELECT
sku_category,
AVG(days_diff) avg_days_diff
FROM customer_purchases
WHERE days_diff > 0
GROUP BY 1
ORDER BY avg_days_diff DESC
