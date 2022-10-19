select id, orderdate, lag(orderdate, 1, orderdate) over(order by orderdate) as previousdate,round(julianday(orderdate) - julianday(lag(orderdate, 1, orderdate) over(order by orderdate)), 2) as differ
from (
    select id, orderdate
    from `order`
    where customerid = 'BLONP'
    limit 10
);