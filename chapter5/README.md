# Recipe 5-4. Perform Joining Operations on Two DataFrames
## Sample Data
```sql

CREATE KEYSPACE pysparksqlbook
	WITH REPLICATION = {'class':'SimpleStrategy','replication_factor':'1'}
	AND DURABLE_WRITES = true;
 
CREATE TABLE "pysparksqlbook"."students"  ( 
	"studentId"	varchar,
	"name" 	varchar,
	"gender"   varchar,
	PRIMARY KEY("studentId")
) WITH
	"caching"={'keys':'NONE', 'rows_per_partition':'NONE'};

INSERT INTO pysparksqlbook.students("studentId", "name", "gender") VALUES('si1', 'Robin', 'M');
INSERT INTO pysparksqlbook.students("studentId", "name", "gender") VALUES('si2', 'Maria', 'F');
INSERT INTO pysparksqlbook.students("studentId", "name", "gender") VALUES('si3', 'Julie', 'F');
insert  into  pysparksqlbook.students("studentId", "name", "gender")  values ('si4', 'Bob',   'M');
insert  into  pysparksqlbook.students("studentId", "name", "gender")    values ('si6','William','M') ;

CREATE TABLE "pysparksqlbook"."subjects"  ( 
    "id"	int,
	"studentId"	varchar,
	"subject" 	varchar,
	"marks"    	int,
	"date"    	date,
	PRIMARY KEY("id")
) WITH
	"caching"={'keys':'NONE', 'rows_per_partition':'NONE'};

insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (1,'si1','Python',75,'2017-01-05') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (2,'si3','Java',76,'2011-05-05') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (3,'si1','Java',81,'2017-01-15') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (4,'si2','Python',85,'2007-05-05') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (5,'si3','Ruby',72,'2017-01-05') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (6,'si4','C++',78,'2015-05-11') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (7,'si5','C',77,'2011-05-05') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (8,'si4','Python',84,'2019-05-07') ;
insert  into  pysparksqlbook.subjects("id","studentId", "subject", "marks","date")    values (9,'si2','Java',83,'2017-07-05') ;
 

```
 print("註冊TempView，才能使用TempView") :
 
 ${DataFrame}.createOrReplaceTempView("students")
  
