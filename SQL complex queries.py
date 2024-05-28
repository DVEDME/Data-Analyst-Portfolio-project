# Databricks notebook source
from delta import DeltaTable
from pyspark.sql import SparkSession
from datetime import datetime

# COMMAND ----------

# MAGIC %md
# MAGIC *ICC tournament*

# COMMAND ----------

spark.sql(f"""create table icc_world_cup
(
Team_1 Varchar(20),
Team_2 Varchar(20),
Winner Varchar(20)
)""")


# COMMAND ----------

spark.sql("""INSERT INTO icc_world_cup values('India','SL','India')
,('SL','Aus','Aus')
,('SA','Eng','Eng')
,('Eng','NZ','NZ')
,('Aus','India','India')""")

# COMMAND ----------

spark.sql("""select * from icc_world_cup""").display()

# COMMAND ----------

spark.sql("""with cte as (
          select team_1 as team  , case when team_1=winner then 1 else 0 end win_flag
           from icc_world_cup
           union all
        select team_2 , case when team_2=winner then 1 else 0  end win_flag_2 from icc_world_cup)
        select * from cte""").show()

# COMMAND ----------

spark.sql("""with cte as (
          select team_1 as team  , case when team_1=winner then 1 else 0 end win_flag
           from icc_world_cup
           union all
        select team_2 , case when team_2=winner then 1 else 0  end win_flag_2 from icc_world_cup)
        select team, count(win_flag) as match_played, sum(case when win_flag=1 then 1 else 0 end) as points from cte group by team""").show()

# COMMAND ----------

# MAGIC %md
# MAGIC *New and repeated customer*

# COMMAND ----------

spark.sql("""create table customer_orders (
order_id integer,
customer_id integer,
order_date date,
order_amount integer
)""")

# COMMAND ----------

spark.sql("""insert into customer_orders values(1,100,cast('2022-01-01' as date),2000),(2,200,cast('2022-01-01' as date),2500),(3,300,cast('2022-01-01' as date),2100)
,(4,100,cast('2022-01-02' as date),2000),(5,400,cast('2022-01-02' as date),2200),(6,500,cast('2022-01-02' as date),2700)
,(7,100,cast('2022-01-03' as date),3000),(8,400,cast('2022-01-03' as date),1000),(9,600,cast('2022-01-03' as date),3000)
""")

# COMMAND ----------

spark.sql("""select * from customer_orders""").show()

# COMMAND ----------

spark.sql("""
          select customer_id, min(order_date) as first_date from customer_orders group by customer_id order by first_date asc""").show()

# COMMAND ----------

spark.sql("""
          with cte as (select customer_id, min(order_date) as first_date from customer_orders group by customer_id order by first_date asc),
          cte2 as (
          select * from customer_orders a join cte  b on a.customer_id=b.customer_id),
          cte3 as (
          select * , case when order_date= first_date then 1 else 0 end as new_customer, 
          case when order_date != first_date then 1 else 0 end as old_customer from cte2 )
          select order_date, sum(new_customer), sum(old_customer) from cte3 group by order_date order by order_date asc
          """).show()

# COMMAND ----------

# MAGIC %md
# MAGIC SQL 3

# COMMAND ----------

spark.sql("""create table entries ( 
name varchar(20),
address varchar(20),
email varchar(20),
floor int,
resources varchar(10))""")

# COMMAND ----------

spark.sql("""insert into entries 
values ('A','Bangalore','A@gmail.com',1,'CPU'),('A','Bangalore','A1@gmail.com',1,'CPU'),('A','Bangalore','A2@gmail.com',2,'DESKTOP')
,('B','Bangalore','B@gmail.com',2,'DESKTOP'),('B','Bangalore','B1@gmail.com',2,'DESKTOP'),('B','Bangalore','B2@gmail.com',1,'MONITOR')
""")

# COMMAND ----------

spark.sql("""
          select * from entries""").show()

# COMMAND ----------

spark.sql("""
          select name, count(*) as total_visits from entries group by name order by name asc
          """).display()

# COMMAND ----------

spark.sql(''' with cte as (
          select name , floor, count(*) as most_visited from entries group by name, floor)
          select name, floor from cte where most_visited=(select max(most_visited) from cte)''').show()

# COMMAND ----------

spark.sql('''
          select name, collect_list(distinct resources) as rooms from entries group by name order by name asc
          ''').display()

# COMMAND ----------

spark.sql(
    '''
    with cte1 as (select name, count(*) as total_visits from entries group by name order by name asc),
    cte2 as (select name , floor, count(*) as most_visited from entries group by name, floor),
    cte4 as( select name, floor as most_visit from cte2 where most_visited=(select max(most_visited) from cte2) ),
    cte3 as (select name, collect_list(distinct resources) as rooms from entries group by name order by name asc)
    select a.name, a.total_visits, b.most_visit, rooms from cte1 a join cte4 b on a.name=b.name join cte3 c on b.name=c.name
    '''
).display()

# COMMAND ----------

# MAGIC %md
# MAGIC Amaazon SQL question

# COMMAND ----------

spark.sql("""create or replace table hospital ( 
          emp_id int
, actions varchar(10)
, Tim timestamp)""")

# COMMAND ----------

spark.sql("""
            insert into hospital 
            values ('1', 'in', '2019-12-22 09:00:00'),
  ('1', 'out', '2019-12-22 09:15:00'),
  ('2', 'in', '2019-12-22 09:00:00'),
  ('2', 'out', '2019-12-22 09:15:00'),
  ('2', 'in', '2019-12-22 09:30:00'),
  ('3', 'out', '2019-12-22 09:00:00'),
  ('3', 'in', '2019-12-22 09:15:00'),
  ('3', 'out', '2019-12-22 09:30:00'),
  ('3', 'in', '2019-12-22 09:45:00'),
  ('4', 'in', '2019-12-22 09:45:00'),
  ('5', 'out', '2019-12-22 09:40:00')
          """)

# COMMAND ----------

spark.sql("""
          select * from hospital
          """).display()

# COMMAND ----------

spark.sql("""
          with cte as (select H.emp_id, actions from hospital H join (select emp_id, max(tim) Ti from hospital group by emp_id) S on H.emp_id=S.emp_id and H.tim=S.Ti)
          select count(*) as total_people_inside from cte where actions='in'
          """).display()

# COMMAND ----------

spark.sql("""create table airbnb_searches 
(
user_id int,
date_searched date,
filter_room_types varchar(200)
)""")

# COMMAND ----------

# spark.sql("""insert into airbnb_searches values
# (1,'2022-01-01','entire home,private room')
# ,(2,'2022-01-02','entire home,shared room')
# ,(3,'2022-01-02','private room,shared room')
# ,(4,'2022-01-03','private room')""")

# COMMAND ----------

# spark.sql("""select * from airbnb_searches""").display()

# COMMAND ----------

# spark.sql("""select split(filter_room_types,',') from airbnb_searches""").display()

# COMMAND ----------

# MAGIC %md
# MAGIC same salary in same department

# COMMAND ----------

spark.sql("""CREATE TABLE emp_salary
(
    emp_id INTEGER  NOT NULL,
    names VARCHAR(20)  NOT NULL,
    salary VARCHAR(30),
    dept_id INTEGER
)""")

# COMMAND ----------

spark.sql("""INSERT INTO emp_salary
(emp_id, names, salary, dept_id)
VALUES(101, 'sohan', '3000', '11'),
(102, 'rohan', '4000', '12'),
(103, 'mohan', '5000', '13'),
(104, 'cat', '3000', '11'),
(105, 'suresh', '4000', '12'),
(109, 'mahesh', '7000', '12'),
(108, 'kamal', '8000', '11')""")

# COMMAND ----------

spark.sql("""select * from emp_salary""").display()

# COMMAND ----------

spark.sql('''
          with cte as (select  dept_id,salary from emp_salary group by  dept_id, salary having count(*) >1 order by dept_id asc)
          select A.* from emp_salary A join cte B on A.dept_id=B.dept_id and A.salary=B.salary order by A.dept_id
          ''').show()

# COMMAND ----------


