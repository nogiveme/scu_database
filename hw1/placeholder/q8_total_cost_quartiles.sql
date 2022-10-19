select CompanyName, CustomerId, round(expenditure, 2) as expenditure
from (select ifnull(CompanyName, 'MISSING_NAME') as CompanyName, CustomerId, expenditure, ntile(4) over(order by expenditure) as pos
from (
    select OrderId, CustomerId, CompanyName, sum(unitprice*quantity) as expenditure
    from (
        select id as OrderId, CustomerId
        from `order`
        )
        natural left outer join (
        select id as CustomerId, CompanyName
        from customer
        )
        natural join
        orderdetail
    group by CustomerId
)) 
where pos = 1;