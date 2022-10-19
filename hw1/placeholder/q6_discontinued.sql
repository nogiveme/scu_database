select productname, companyname, contactName
from (
    select *, min(orderdate) as first_order
    from product as A, orderdetail as B, `order` as C, customer as D
    where A.id = B.productid
        and A.discontinued = 1
        and B.orderid = C.id
        and C.customerid = D.id
    group by productname
) as R
where R.first_order = R.orderdate;