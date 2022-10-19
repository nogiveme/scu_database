select sn, substr(sn, 1, instr(sn,'-')-1) as fn
from (select distinct shipname as sn
from `order`
where shipname like '%-%'
order by shipname);