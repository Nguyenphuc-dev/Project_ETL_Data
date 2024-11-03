from pyspark.sql import SparkSession
import pyspark.sql.functions as sf
import datetime
import time
spark = SparkSession.builder.config('spark.jars.packages','com.datastax.spark:spark-cassandra-connector_2.12:3.1.0').getOrCreate()

def retreive_from_cassandra(table,keyspace):
    data = spark.read.format("org.apache.spark.sql.cassandra").options(table = 'tracking_longth',keyspace = 'de_prj').load()
    return data

def solve_new_data():
    data = retreive_from_cassandra('tracking_longth','de_prj')
    data = data.select("ts","job_id","bid","campaign_id","custom_track","group_id","publisher_id")
    new_data = data.filter(data.custom_track.isNotNull())
    return new_data
def solve_data():
    new_data = solve_new_data()
    #Process clickdata
    click_data = new_data.filter(new_data.custom_track == 'click')
    click_data.createTempView("_clickdata_")
    click_data = spark.sql("""select date(ts) as date,hour(ts) as hour,job_id,publisher_id,campaign_id,group_id,round(avg(bid),2) as bid_set,sum(bid) as spend_hour,count(*) as click 
                       from _clickdata_ 
                       group by date(ts),hour(ts),job_id,publisher_id,campaign_id,group_id""")
    # Process conversion
    conversion_data = new_data.filter(new_data.custom_track == 'conversion')
    conversion_data.createTempView("_conversiondata_")
    conversion_data = spark.sql("""select date(ts) as date,hour(ts) as hour,job_id,publisher_id,campaign_id,group_id,count(*) as conversion 
                       from _conversiondata_
                       group by date(ts),hour(ts),job_id,publisher_id,campaign_id,group_id""")
    # Process qualified
    qualified_data = new_data.filter(new_data.custom_track == 'qualified')
    qualified_data.createTempView("_qualifieddata_")
    qualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour,job_id,publisher_id,campaign_id,group_id,count(*) as qualified 
                       from _qualifieddata_
                       group by date(ts),hour(ts),job_id,publisher_id,campaign_id,group_id""")
    # Process unqualified
    unqualified_data = new_data.filter(new_data.custom_track == 'unqualified')
    unqualified_data.createTempView("_unqualifieddata_")
    unqualified_data = spark.sql("""select date(ts) as date,hour(ts) as hour,job_id,publisher_id,campaign_id,group_id,count(*) as unqualified 
                       from _unqualifieddata_
                       group by date(ts),hour(ts),job_id,publisher_id,campaign_id,group_id""")
    # Finalize output full join
    result = click_data.join(conversion_data,on=["date","hour","job_id","publisher_id","campaign_id","group_id"],how="full").\
                        join(qualified_data,on=["date","hour","job_id","publisher_id","campaign_id","group_id"],how="full").\
                        join(unqualified_data,on=["date","hour","job_id","publisher_id","campaign_id","group_id"],how="full")
    return result

def solve_result():
    result = solve_data()
    new_data = solve_new_data()
    result = result.withColumnRenamed("date","dates")
    result = result.withColumnRenamed("hour","hours")
    result = result.withColumnRenamed("click","clicks")
    result = result.withColumnRenamed("qualified","qualified_application")
    result = result.withColumnRenamed("unqualified","disqualified_application")
    result = result.withColumn("sources",sf.lit("Cassandra"))
    result = result.withColumn("updated_at",sf.lit(new_data.select(sf.max("ts")).take(1)[0][0].split(".")[0]))
    return result
    
def retreive_from_mysql():
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_data'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '111111'
    sql = '(SELECT id AS job_id,company_id FROM job) A'
    jobs = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()
    return jobs

def solve_final():
    jobs = retreive_from_mysql()
    result = solve_result()
    final = result.join(jobs,on="job_id",how="left")
    final = final.select(
    'job_id', 
    'dates', 
    'hours', 
    'disqualified_application', 
    'qualified_application', 
    'conversion', 
    'company_id', 
    'group_id', 
    'campaign_id', 
    'publisher_id', 
    'bid_set', 
    'clicks', 
    'spend_hour', 
    'sources', 
    'updated_at'
    )
    return final

def write_data_mysql():
    final = solve_final()
    final.write.format("jdbc") \
    .option("driver", "com.mysql.cj.jdbc.Driver") \
    .option("url", "jdbc:mysql://localhost:3306/etl_data") \
    .option("dbtable", "events") \
    .mode("append") \
    .option("user", "root") \
    .option("password", "111111") \
    .save()

def get_lastest_time_cassandra():
    new_data = solve_new_data()
    max_time_cassandra = new_data.select(sf.max("ts")).take(1)[0][0].split(".")[0]
    return max_time_cassandra

def get_lastest_time_mysql():
    url = 'jdbc:mysql://' + 'localhost' + ':' + '3306' + '/' + 'etl_data'
    driver = "com.mysql.cj.jdbc.Driver"
    user = 'root'
    password = '111111'
    sql = "(SELECT max(updated_at) AS updated_at FROM events) A" 
    time_mysql = spark.read.format('jdbc').options(url = url , driver = driver , dbtable = sql , user=user , password = password).load()
    
    result = time_mysql.take(1)
    # Check if we got any results and if the first value is None
    if not result or result[0][0] is None:
        max_time_mysql = '1998-01-01 23:59:59'
    else:
        max_time_mysql = datetime.datetime.strftime(result[0][0], "%Y-%m-%d %H:%M:%S")
    
    return max_time_mysql

while True:
    start_time = datetime.datetime.now()
    max_time_cassandra = get_lastest_time_cassandra()
    max_time_mysql = get_lastest_time_mysql()
    if max_time_cassandra > max_time_mysql:
        write_data_mysql()
    else:
        print("No data new found")
    end_time = datetime.datetime.now()
    execution_time = (end_time-start_time).total_seconds()
    print('Job takes {} seconds to execute'.format(execution_time))
    time.sleep(10)



