--############################ v2 v2 v2 v2 v2 v2 v2 ########################  building DQ
drop table if exists `Sample.DQ_rules`;
create table `Sample.DQ_rules`(
DQ_ID INT64 not null,
DQ_SID INT64 not null,
Checks string not null,
SQLS string not null,
Active INT64 not null
);

insert into Sample.DQ_rules values (1,1,'Null Check on KPIs ','insert into Sample.DQ_rules_RPT with er as (select count(*) as cnt  from Sample.emp  where comm is null) select 1,\'Number Of Null Observations for employee comm:- \',er.cnt,null,current_timestamp from er',1);

insert into Sample.DQ_rules values (2,1,'Anomaly Check on Emp Sal','insert  into Sample.DQ_rules_RPT with er as (select count(*) as cnt  from Sample.emp  where sal>2500) select 2,\'Number of anomaly records (high employee sal ):-\',er.cnt,null,current_timestamp from er',1);

insert into Sample.DQ_rules values (3,1,'co rel dept --sal','insert into Sample.DQ_rules_RPT with er as (select CORR(deptno,sal) as cnt  from Sample.emp  ) select 3,\'corel dept and sal is :-\',er.cnt,null,current_timestamp from er',1);



---------------------- one time table creation needed in deployment 
drop table if exists `Sample.DQ_rulesT`;
CREATE TABLE `Sample.DQ_rulesT` AS
select * from 
(SELECT DQ_ID,checks,SQLS, RANK() OVER(ORDER BY DQ_ID,DQ_SID) DQ_ExeSeq,0 as DQ_runseq, 0 as last_run_suc_flg
FROM `Sample.DQ_rules`  where 1=2) SQ
;

----------------------  DQ log table 
drop table if exists `Sample.DQ_rules_RPT`;
Create table `Sample.DQ_rules_RPT` (
DQ_SID  INT64 not null,
Checks string not null,
Value_CNT NUMERIC not null,
Results INT64,  --- 1=pass,0 fail
ins_dt TIMESTAMP,
run_bth_id INT64
);
--------------  log table
drop table if exists `Sample.DQ_rules_log`;
create table `Sample.DQ_rules_log`
(
run_status string,
qid INT64 ,
ins_dt TIMESTAMP,
run_bth_id INT64
);
------------------- err table 
drop table if exists `Sample.DQ_err_log`;
create table `Sample.DQ_err_log`
(
err_message string,
err_statement_text string,
ins_dt TIMESTAMP,
run_bth_id INT64
);
-------------------------- seq object 
drop table if exists  `Sample.DQ_seq`;
create table `Sample.DQ_seq` (
DQ_runseq INT64
);
insert into `Sample.DQ_seq` values (0);
------------------------
drop procedure if exists `Sample.DQ_rules`;
CREATE PROCEDURE `Sample.DQ_rules`(ErrProc INT64)
BEGIN
--------   declaring var
DECLARE x INT64 DEFAULT 1;
DECLARE z INT64 DEFAULT 0;
declare vsql string;
declare v_err_message string;
declare v_err_statement_text string;
declare v_DQ_runseq INT64;
DECLARE v_ErrProc INT64 DEFAULT ErrProc; -- define restartibility -- ErrProc = 1 - error process , when 0 then normal DQ process--
-----------------only for err reprocess 
If v_ErrProc=1 then 
CREATE OR REPLACE TABLE `Sample.DQ_rulesT`
AS SELECT * FROM `Sample.DQ_rulesT`  LIMIT 0;
else
select 99 ;
end if ;
-------------------  assign seq for run
set v_DQ_runseq= (select DQ_runseq+1 from `Sample.DQ_seq`);
---------  keep active DQ for each run 
IF EXISTS (SELECT 1 FROM `Sample.DQ_rulesT`
           WHERE last_run_suc_flg=0) THEN
CREATE TABLE `Sample.DQ_rulesT_T` AS
select * from 
(SELECT DQ_ID,checks,SQLS, RANK() OVER(ORDER BY DQ_ExeSeq) DQ_ExeSeq,DQ_runseq as DQ_runseq, last_run_suc_flg
FROM `Sample.DQ_rulesT`  where last_run_suc_flg=0) SQ
order by SQ.DQ_ExeSeq;

drop table `Sample.DQ_rulesT`;
create table `Sample.DQ_rulesT` as select * from `Sample.DQ_rulesT_T` order by DQ_ExeSeq;
drop table `Sample.DQ_rulesT_T`;

else
drop table if exists `Sample.DQ_rulesT`;
CREATE TABLE `Sample.DQ_rulesT` AS
select * from 
(SELECT DQ_ID,checks,SQLS, RANK() OVER(ORDER BY DQ_ID,DQ_SID) DQ_ExeSeq,v_DQ_runseq as DQ_runseq , 0 as last_run_suc_flg
FROM `Sample.DQ_rules`  where Active=1) SQ
order by SQ.DQ_ExeSeq;

END IF;
--------  get ready for loop
SET z= (SELECT COUNT(*) FROM `Sample.DQ_rulesT`);
--------  starting loop 
WHILE x<=z DO
-------------------------------------  setting the DQ rule SQL to var and execute 
SET vsql= (select SQLS from `Sample.DQ_rulesT` where DQ_ExeSeq=x);
insert into `Sample.DQ_rules_log` values ('Successfully initiated the DQ rule sequence :-#',x,current_timestamp,v_DQ_runseq);

begin
EXECUTE IMMEDIATE  vsql;
update `Sample.DQ_rules_RPT` set run_bth_id=v_DQ_runseq where run_bth_id is null ;
update `Sample.DQ_rulesT` set last_run_suc_flg=1 where DQ_ExeSeq=x;
EXCEPTION WHEN ERROR THEN
insert into `Sample.DQ_err_log` values (@@error.message,@@error.statement_text,current_timestamp,v_DQ_runseq);
end;

insert into `Sample.DQ_rules_log` values ('Successfully completed the DQ rule sequence:-#',x,current_timestamp,v_DQ_runseq);
-------------------- checking till last looping
SET x=x+1;

END WHILE;
---------------  holding the sequence unchanged 
IF EXISTS (SELECT 1 FROM `Sample.DQ_rulesT`
           WHERE last_run_suc_flg=0) THEN
select 9999;
ELSE 

update `Sample.DQ_seq` set DQ_runseq=v_DQ_runseq  where 1=1;
END IF;
---------- if  block ends 
END;

CALL `Sample.DQ_rules`(0);

---##################################
select * from `Sample.DQ_rules`;
select * from `Sample.DQ_rulesT`;
select * from `Sample.DQ_rules_RPT`;
select * from `Sample.DQ_rules_log`;
select * from  `Sample.DQ_err_log`;


