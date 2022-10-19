select Id, ShipCountry, case when ShipCountry='Canada' or ShipCountry='Mexico' or ShipCountry='USA' then 'NorthAmerican' else 'OtherPlace' end as 'Place'
from `order`
where Id >= 15445
order by Id
limit 20;