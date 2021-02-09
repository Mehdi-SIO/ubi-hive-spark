SELECT ltd_table.customerid AS customerid, order_date, order_datetime, address, amount_eur, ip_address_ltd, amount_eur_utd
from (SELECT DISTINCT customerid, address, order_date, order_datetime, amount_eur, sum(amount_eur)
      over(
          partition by customerid
          order by order_datetime ASC
          rows between unbounded preceding and current row) amount_eur_utd
      FROM schematransaction) as utd_table
      INNER JOIN (SELECT  customerid, count(DISTINCT ip_address) as ip_address_ltd
      FROM schematransaction
      GROUP BY customerid) as ltd_table
      WHERE utd_table.customerid=ltd_table.customerid



