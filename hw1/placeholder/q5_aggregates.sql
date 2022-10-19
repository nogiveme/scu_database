select categoryname, count(*),round(avg(unitprice),2), min(unitprice), max(unitprice), sum(unitsonorder)
from (select categoryid, count(*) as Contain
    from product
    group by categoryid)
    natural join 
    product,
    category
where contain > 10 and category.id = categoryid
group by categoryid
order by categoryid;