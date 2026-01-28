/*
chcp 65001 && spark-shell -i \Users\Ekaterina\ETL\seminar_2\s2_new.scala --conf "spark.driver.extraJavaOptions=-Dfile.encoding=utf-8"
*/
import org.apache.spark.internal.Logging
import org.apache.spark.sql.functions.{col, collect_list, concat_ws}
import org.apache.spark.sql.{DataFrame, SparkSession}
import org.apache.spark.sql.functions._
import org.apache.spark.sql.expressions.Window
val t1 = System.currentTimeMillis()
val misqlcon = "jdbc:mysql://localhost:3306/spark?user=root&password=24082019:jhf"
val driver = "com.mysql.cj.jdbc.Driver" 
if(1==1){
var df1  = spark.read
        .option("header", "true")                                                
        .option("delimiter", ",")
				.option("encoding", "utf-8")                     
        .csv("/Users/Ekaterina/ETL/seminar_2/s2_data.csv") 
		    
				df1=df1
     		.withColumn("children",col("children").cast("int"))
				.withColumn("dob_years",col("dob_years").cast("int"))
				.withColumn("family_status",col("family_status").cast("int"))
				.withColumn("education_id",col("education_id").cast("int"))
				.withColumn("debt",col("debt").cast("int"))
         	
				.withColumn("days_employed",col("days_employed").cast("float")) 
        .withColumn("total_income",col("total_income").cast("float")).dropDuplicates()
				.withColumn("purpose_category", 
		    when(col("purpose").like("%авто%"),"операции с автомобилем")
        when(col("purpose").like("%недвиж%")||col("purpose").like("%жиль%"),"операции с недвижимостью")
        /*when(col("purpose").like("%свадьб%"),"проведение свадьбы")
				when(col("purpose").like("%образ%"),"получение образования")
				.otherwise("Другое")*/
				)
				.withColumn("total_income2",
				when(col("total_income").isNotNull,col("total_income"))
				.otherwise(avg("total_income").over(Window.partitionBy("income_type").orderBy("income_type"))))
				.withColumn("total_income2",col("total_income2").cast("float"))
		
		df1.write.format("jdbc")
		.option("url", misqlcon)
	  .option("driver", driver)
		.option("dbtable", "tasketl2b")
    .option("user", "root")
    .option("password", "24082019:jhf")
    .option("charset", "utf8mb4_general_ci")  
    .mode("overwrite")
    .save()
		
		df1.show()

val s = df1.columns.map(c => sum(col(c).isNull.cast("integer")).alias(c))
val df2 = df1.agg(s.head, s.tail:_*)
val t = df2.columns.map(c => df2.select(lit(c).alias("col_name"), col(c).alias("null_count")))
val df_agg_col = t.reduce((df1, df2) => df1.union(df2))
df_agg_col.show()
}

import java.sql._;
def sqlexecute(sql:String) = {
	  var conn:Connection=null;
		var stmt:Statement=null;
		try {
			Class.forName(driver)
			conn=DriverManager.getConnection(misqlcon)
			stmt=conn.createStatement();
			stmt.executeUpdate(sql);
			println(sql+" complete")
			} catch {
				case e: Exception =>println(e);
				}
}
def checktable(scheme:String,table:String) = {
	  var conn:Connection=null;
		var dmd:DatabaseMetaData=null;
		var rs:ResultSet=null;
		try {
			Class.forName(driver)
			conn=DriverManager.getConnection(misqlcon)
			dmd=conn.getMetaData();
			rs=dmd.getTables(scheme,null,table,null)
			rs.next()
			println(rs.getString("TABLE_NAME")+" exists")
			} catch {
				case e: Exception =>println(table+" doesn't exist");
				}
}

val ct = """CREATE TABLE if not exixts 'task4' (
	'Отдел' TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	'Начальник' TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci',
	'Сотрудники' TEXT NULL DEFAULT NULL COLLATE 'utf8mb4_0900_ai_ci'
)
COLLATE='utf8mb4_0900_ai_ci'
ENGINE=InnoDB
;"""

val data = Seq("Кадры","Иванов","Петров")
sqlexecute(s"insert into task4 VALUES ('${data(0)}','${data(1)}','${data(2)}')")
val df1 = sc.parallelize(List(("ddd","fff", "ggg"))).toDF("Отдел", "Начальник", "Сотрудники")
df1.show()
df1.write.format("jdbc").option("url", misqlcon)
        .option("driver", driver).option("dbtable", "task4")
				.mode("append").save()
				
val s0 = (System.currentTimeMillis() - t1)/1000
val s = s0 % 60
val m = (s0/60) % 60
val h = (s0/60/60) % 24
println("%02d:%02d:%02d".format(h, m, s))


System.exit(0)