select CompanyName, round(late_order * 1.0 / all_order * 100, 2) as Percent
from (select shipvia, count(*) as all_order
    from `order`
    group by shipvia)
    natural join
    (select shipvia, count(*) as late_order
    from `order`
    where shippeddate > requireddate
    group by shipvia),
    shipper
where shipper.id = shipvia;