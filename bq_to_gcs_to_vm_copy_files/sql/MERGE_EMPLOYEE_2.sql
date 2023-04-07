WITH upsert as(update employee_2 t2 set empfname=t1.empfname,emplname= t1.emplname  from staging_employee_2 t1 where t1.empcode = t2.empcode RETURNING t2.*) insert into employee_2 select p.empcode, p.empfname from staging_employee_2 p where p.empcode not in (select q.empcode from upsert q);