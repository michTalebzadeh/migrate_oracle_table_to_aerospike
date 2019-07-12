import java.sql.DriverManager
import java.sql.Connection
import java.sql.DatabaseMetaData
import java.sql.ResultSet
import java.sql.SQLException
import java.util.ArrayList

import org.apache.spark.sql.{ SQLContext, SparkSession, SaveMode}
import org.apache.spark.SparkConf

import org.apache.log4j.Logger
import org.apache.log4j.Level
Logger.getLogger("org").setLevel(Level.OFF)
Logger.getLogger("akka").setLevel(Level.OFF)
sc.setLogLevel("ERROR")

//
//aerospike stuff
//
import com.aerospike.spark.sql._
import com.aerospike.client.AerospikeClient
import com.aerospike.client.Bin
import com.aerospike.client.Key
import com.aerospike.client.Value
import com.aerospike.client.AerospikeClient
import com.aerospike.client.Host
import com.aerospike.client.policy.ClientPolicy

import org.apache.spark.sql.functions._
import org.apache.spark.sql.Column
def changeToDate (TRANSACTIONDATE : String) : org.apache.spark.sql.Column =  {
    to_date(unix_timestamp($"TRANSACTIONDATE","dd/MM/yyyy").cast("timestamp"))
}

def emptyToNullString(c: Column) = when(length(trim(c)) > 0, c).otherwise("---")

def remove_string: String => String = _.replaceAll("[,_#]", "")
def remove_string_udf = udf(remove_string)
def remove_map: Map[String, Int] => Map[String, Int] = _.map{ case (k, v) => k.replaceAll("[,_#]", "|") -> v }
def remove_map_udf = udf(remove_map)
val toInt    = udf[Int, String]( _.toInt)
val toDouble = udf[Double, String]( _.toDouble)
val toHour   = udf((t: String) => "%04d".format(t.toInt).take(2).toInt ) 
val days_since_nearest_holidays = udf( 
  (year:String, month:String, dayOfMonth:String) => year.toInt + 27 + month.toInt-12
 )

val HiveContext = new org.apache.spark.sql.hive.HiveContext(sc)
val driverName = "oracle.jdbc.OracleDriver"
var url= "jdbc:oracle:thin:@rhes564:1521:mydb12"
var _username = "scratchpad"
var _password = "xxxxx"
var _dbschema = "SCRATCHPAD"
var _dbtable = "DUMMY"
var e:SQLException = null
var connection:Connection = null
var metadata:DatabaseMetaData = null

// Check Oracle is accessible
try {
      connection = DriverManager.getConnection(url, _username, _password)
} catch {
  case e: SQLException => e.printStackTrace
  connection.close()
}
metadata = connection.getMetaData()
// Check table exists
var rs:ResultSet = metadata.getTables(null,_dbschema,_dbtable, null)

if (rs.next()) {
   println("Table " + _dbschema+"."+_dbtable + " exists")
} else {
   println("Table " + _dbschema+"."+_dbtable + " does not exist, quitting!")
   connection.close()
   sys.exit(1)
}
//
// Test all went OK by looking at some old transactions
//
//
val df = HiveContext.read.format("jdbc").options(
       Map("url" -> url,
       "dbtable" -> "(SELECT ID, CLUSTERED, SCATTERED, RANDOMISED, RANDOM_STRING, SMALL_VC, PADDING FROM scratchpad.dummy where ROWNUM <= 10000)",
       "user" -> _username,
       "password" -> _password)).load

//
// convert columns to correct types
val df1 = df.
          withColumn("ID", toDouble(df("ID"))).
          withColumn("CLUSTERED", toDouble(df("CLUSTERED"))).
          withColumn("SCATTERED", toDouble(df("SCATTERED"))).
          withColumn("RANDOMISED", toDouble(df("RANDOMISED"))).
          select('ID,'CLUSTERED,'SCATTERED,'RANDOMISED,'RANDOM_STRING,'SMALL_VC,'PADDING).orderBy("ID")
df1.printSchema
df1.take(5).foreach(println)
var hosts = {
    new Host("rhes75", 3000)
}

var dbHost = "rhes75"
var dbPort = "3000"
var dbConnection = "test_RW"
var namespace = "test"
var dbPassword = "xxxx"
var dbSet = "oracletoaerospike"

     val sqlContext = spark.sqlContext

     spark.conf.set("aerospike.seedhost", dbHost)
     spark.conf.set("aerospike.port", dbPort)
     spark.conf.set("aerospike.namespace",namespace)
     spark.conf.set("aerospike.set", dbSet)
     spark.conf.set("aerospike.keyPath", "/etc/aerospike/features.conf")
     spark.conf.set("aerospike.user", dbConnection)
     spark.conf.set("aerospike.password", dbPassword)


  df1.write.
      mode(SaveMode.Overwrite). 
      format("com.aerospike.spark.sql").
      option("aerospike.updateByKey", "ID").
      option("aerospike.keyColumn", "__ID").
      save()

   val dfRead  = sqlContext.read.
      format("com.aerospike.spark.sql").
      option("aerospike.batchMax", 10000).
      load
dfRead.select('ID,'CLUSTERED,'SCATTERED,'RANDOMISED,'RANDOM_STRING,'SMALL_VC,'PADDING).orderBy("ID").take(5).foreach(println)
//
println ("\nFinished at"); spark.sql("SELECT FROM_unixtime(unix_timestamp(), 'dd/MM/yyyy HH:mm:ss.ss') ").collect.foreach(println)
sys.exit()
