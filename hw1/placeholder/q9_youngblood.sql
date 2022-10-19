select regiondescription, firstname, lastname, birthdate
from (select regiondescription, firstname, lastname, birthdate, max(birthdate) as ma
from employee as A, employeeterritory as B, territory as C, region as D
where A.id = B.employeeid
    and B.territoryid = C.id
    and C.regionid = D.id
group by D.id
)
where birthdate = ma;