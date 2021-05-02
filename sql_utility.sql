#### Handling date
---Hive
date_trunc('month', to_date(datestr)) as mon_bg_dt 
date(datestr) between date_sub('{{ds}}', 121) and date('{{ds}}')

DATE_DIFF('minute', request_timestamp_utc, begintrip_timestamp_utc) as req_begin_minutes
DATEDIFF(TO_DATE(transaction_time), TO_DATE(request_at)) AS bill_diff_date,

add_months(trunc(current_date,'MM'),-6)

cast(from_unixtime(cd.created_at/1000) as date)
to_char(date_trunc('MONTH', FROM_UNIXTIME(cast(md.ts as BIGINT))), 'yyyy-MM-dd') as month

case when week_starting_date = DATE_SUB(default.date_trunc('week',current_date),7) then 'Last Week' 
              when week_starting_date = DATE_SUB(default.date_trunc('week',current_date),14) then 'Previous Week' 
              else 'NA' 
         end recent_week_flag

---Presto
cast(datestr as date)>= current_date - interval '9' month
DATE_FORMAT(date_trunc('day', t.begintrip_timestamp_utc ), '%Y-%m-%d')  as trip_date,


#### Json object
---Hive
cast(get_json_object(mf_features_json, '$.account_age_in_seconds') as bigint)

---presto

CAST(JSON_EXTRACT_SCALAR(mf_features_json, '$.account_age_in_seconds') AS DOUBLE) < 60*60*24*14

##### Array

select 
    `_row_key` as trip_uuid
    , all_points.epoch
    , all_points.latitude
    , all_points.longitude
    , all_points.speed
from rawdata.schemaless_mezzanine_trips_cells 
 LATERAL VIEW explode(route.all_points) all_points_table as all_points   ----Array
where route is not null 
    and route.city_id = 1 
    and datestr = '2016-11-01'
    and `_row_key` = '05cde1d0-cbf7-4869-b53d-f437ea86db7f'


#### calculate distance 
max(esri.ST_DISTANCE(
	esri.ST_SETSRID(esri.ST_POINT(trips.base.request_lng, trips.base.request_lat), 432),
	esri.ST_SETSRID(esri.ST_POINT(events.msg.location.lng, events.msg.location.lat), 432)
	)) as request_dist

####  Regular expression

REGEXP_REPLACE(hbom.content, '\n', ' ') AS content,

NOT REGEXP_LIKE(COALESCE(global_product_name, ''), 'EATS|RUSH|TEST') 


#### Window function

select lag(a, 1) over (order by a) as previous, a, lead(a, 1) over (order by a) as next from foo;

SELECT a, SUM(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW)
FROM T;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND CURRENT ROW)
FROM T;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN 3 PRECEDING AND 3 FOLLOWING)
FROM T;
SELECT a, AVG(b) OVER (PARTITION BY c ORDER BY d ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING)
FROM T;


ROW_NUMBER() OVER (PARTITION BY trip_uuid, payment_uuid ORDER BY created_at desc) as qrk

SUM(COALESCE(cb_tot,0)) OVER (PARTITION BY aa.mega_region, aa.marketplace, aa.trip_date ORDER BY aa.bill_diff_date ROWS BETWEEN UNBOUNDED PRECEDING AND CURRENT ROW) as cum_cb_tot


---Median
select mon_bg_dt, mega_region, country_name, n_trips, APPROX_PERCENTILE(n_trips,0.5) OVER () as n_trips_median
from (
select date_trunc('month', date(datestr)) as mon_bg_dt
 , mega_region
 , country_name 
 , count(distinct trip_uuid) as n_trips 
from fraud.pmt_risk_cohort_fact 
where datestr >= '2021-01-01' and datestr <'2021-01-07'
and  marketplace in ('agora', 'eats')
group by 1,2,3 
) aa 
group by 1,2,3,4

----Mode - most frequent user segments with CBs
select mon_bg_dt, mega_region, country_name,
rider_risk_user_segment_name
from (
select date_trunc('month', date(datestr)) as mon_bg_dt
 , mega_region
 , country_name 
 , rider_risk_user_segment_name, sum(chargeback_count) as CB
 , ROW_NUMBER() OVER (PARTITION BY country_name ORDER BY sum(chargeback_count) DESC) as rk
from fraud.pmt_risk_cohort_fact 
where datestr >= '2021-01-01' and datestr <'2021-01-07'
and  marketplace in ('agora', 'eats')
and chargeback_count = 1 
group by 1,2,3,4 
) aa 
where rk=1

----Mode - most frequent user segments with CBs
select mon_bg_dt, mega_region, country_name,
MAP_KEYS(hist)[
    ARRAY_POSITION(MAP_VALUES(hist), ARRAY_MAX(MAP_VALUES(hist)))
] as most_freq_user
from (
select date_trunc('month', date(datestr)) as mon_bg_dt
 , mega_region
 , country_name 
 , HISTOGRAM (rider_risk_user_segment_name) as hist
from fraud.pmt_risk_cohort_fact 
where datestr >= '2021-01-01' and datestr <'2021-01-07'
and  marketplace in ('agora', 'eats')
and chargeback_count = 1 
group by 1,2,3 
) aa 


##### create table 

----Create table from CSV file
---1. Copy file from local to hadoop server
scp tablefile.csv guoyu.zhu@hadoopgw01-sjc1:/home/guoyu.zhu/

---2. Remove header from your CSV file 
sed '1,1d' chargeback_loss_proj_factor_w_header.csv > chargeback_loss_proj_factor.csv

---3. Create table in the hadoop
hadoop fs -mkdir /user/your_ldap
hadoop fs -ls /user/your_ldap/
hadoop fs -mkdir /user/your_ldap/table_name

hadoop fs -rm /user/ldap/table_name/tablefile.csv
hadoop fs -put tablefile.csv  /user/ldap/table_name


create external table if not exists fraud.finance_loc_hierarchy (
City_id int,
Code int,
LeafLevelOralceLocation string,
PU  string,
SubRegion  string,
Region  string,
RegionGroup  string,
MegaRegion   string,
SuperRegion  string,
Country  string
)
ROW FORMAT DELIMITED
FIELDS TERMINATED BY ','
STORED AS TEXTFILE
location '/user/guoyu/finance_loc_hierarchy/';

####spark_hive_setup = """
SET spark.yarn.queue=fraud-adhoc;
set hive.execution.engine=spark;
set spark.executor.memory=7168m;
set spark.executor.cores=2;
set spark.executor.instances=200;
set spark.yarn.executor.memoryOverhead=6072;
set hive.exec.compress.output=true;
set hive.strict.checks.large.query=false;
set mapred.output.compression.type=BLOCK;
set mapred.min.split.size=67108864;
set mapred.output.compression.codec=org.apache.hadoop.io.compress.SnappyCodec;
set hive.auto.convert.join = false;
set hive.auto.convert.join.noconditionaltask = true;
set hive.auto.convert.join.noconditionaltask.size = 10000000;
set hive.exec.parallel=true;
set hive.exec.dynamic.partition=true;
set hive.exec.dynamic.partition.mode=nonstrict;
set hive.exec.reducers.bytes.per.reducer=536870912;
set hive.exec.reducers.max=500;
set hive.exec.compress.intermediate=true;
set hive.exec.max.dynamic.partitions.pernode=10000;
set hive.exec.max.dynamic.partitions=10000;
set hive.merge.size.per.task=536870912;
set hive.merge.smallfiles.avgsize=134217728;
set hive.optimize.sort.dynamic.partition=true;


create table if not exists fraud.loss_forecast_proj_dynamic_cb_trip_evt_data (
 city_id         int,
 country_id      bigint,
 mega_region     string,
 region          string,
 eats_flow       string,
 marketplace     string,
 u4b_flag        string,
 region_ldf      string,
 ft_trip_uuid    string,
 bill_uuid       string,
 charge_date     date,
 charge_month    string,
 charge_age      int,
 trip_date       date,
 trip_month      string,
 trip_age        int,
 dispute_date    date,
 dispute_month   string,
 win             int,
 adj_state       string,
 job_type        string,
 cb_total        double
) PARTITIONED BY (datestr string);

with go1 as (
)
, go2 as (
)

INSERT OVERWRITE TABLE fraud.loss_forecast_proj_dynamic_cb_trip_evt_data PARTITION(datestr='{{ds}}')
	select
		co.city_id,
		country_id,
		cy.mega_region,
		cy.region,
		case when lower(co.marketplace) LIKE '%eats%' then 'Y' else 'N' end as Eats_flow,
		co.marketplace,
		case when uber_everything_product_type is null or uber_everything_product_type = 'U4B-Unmanaged' then 'Core'
						when uber_everything_product_type like 'U4B-Managed%' then 'U4B'
						when uber_everything_product_type = 'U4F' then 'U4F'
						else 'Other' end as U4B_flag,

		case when lower(co.marketplace) LIKE '%eats%' then 'Others'
										WHEN  cy.mega_region = 'US & Canada' THEN 'US & Canada'
										WHEN  cy.mega_region = 'EMEA' THEN 'EMEA'
										WHEN  cy.mega_region in ('ANZ', 'SENA','APACX') THEN 'APACX'
										WHEN  cy.mega_region = 'LatAm' THEN 'LatAm'
										else 'Others' end as region_ldf,
		cb.trip_uuid as ft_trip_uuid,
		cb.bill_uuid,
		TO_DATE(transaction_date) as charge_date,
		date_trunc('month', transaction_date)as charge_month,
		DATEDIFF(current_date, transaction_date) AS charge_age,
		To_date(co.datestr) as trip_date,
		date_trunc('month', TO_DATE(co.datestr)) as trip_month,
		DATEDIFF(current_date, TO_DATE(co.datestr)) as trip_age,
		TO_DATE(dispute_date) as dispute_date,
		date_trunc('month', TO_DATE(dispute_date)) as dispute_month,
		case when cb.adj_state = 'Representment Won' then 1 else 0 end as win,
		adj_state,
		job_type,
		abs(bill_amt_usd) as cb_total
	from cb
	left join co on cb.trip_uuid=co.trip_uuid
	left join dwh.dim_city cy on cy.city_id = co.city_id

