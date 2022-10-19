select group_concat(productname) as productname
from 
(
select productname
from product, orderdetail, `order` as A, customer
where product.id = orderdetail.productid
    and orderdetail.orderid = A.id
    and A.customerid = customer.id
    and customer.companyname = 'Queen Cozinha'
    and orderdate like '2014-12-25%'
order by productid);