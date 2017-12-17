package BigData.Spark

import org.apache.spark.SparkContext

import org.apache.spark.SparkConf

import org.apache.spark.sql.types._

import org.apache.spark.sql.SQLContext

import org.apache.spark.sql.SparkSession

import org.apache.spark.rdd.RDD

import org.apache.spark.rdd.DoubleRDDFunctions

import org.apache.spark.sql.Row

import org.apache.spark.sql.Column

import org.apache.spark.sql.functions

import org.apache.spark.sql.DataFrameStatFunctions

import org.apache.spark.sql.Dataset

import org.elasticsearch.spark._

import org.elasticsearch.hadoop.cfg.ConfigurationOptions._

import java.util.Calendar

import java.text.SimpleDateFormat

object Q1 {
  
  def ReadCSV(spark: SparkSession, id:String, args:String, args3:String, args4: Boolean, objectName:String): Unit = {
    
    import spark.implicits._ 
    
    val df = spark.read.option("header", args4).option("delimiter", args3).csv(args)
    val fieldNames = df.schema.fields
    
    //Converting RDD into an array for Traversal
    val FirstRow = df.first()
    val FC= FirstRow.length
    
    //Renaming Columns for Ease of Iteration
    var newNames = Seq("_c0")
    for(i <- 1 until FC)
    newNames =  newNames :+ "_c"+i
    val dfRenamed = df.toDF(newNames: _*)
   
    //Creating table for queries
    dfRenamed.createOrReplaceTempView("people")
    
    // Counting Total records in the RDD
    val TotalRecords = df.count() 
    
    var TypeArray = new Array[String](FC)
    
    //Checking for Field Type
    
    for( i <- 0 until FC ){    
      val results = spark.sql("SELECT SUM(LENGTH(TRIM(TRANSLATE(_c"+i+",' +-.0123456789','a ')))) FROM people")
      
      val FinalResults = results.head().getLong(0)
      
      if(FinalResults==0){
        TypeArray(i)= "Numeric"
      }
      else
      {
        TypeArray(i)= "String"
      }
    }
    
    //TimeStamp
    val timestamp= Calendar.getInstance.getTime
    
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    
    val before= dateFormat.format(timestamp)
    
    var json= "{\"id\": \""+id+"\",\"parameters\": {\"location\": \""+args+"\",\"type\": \"delimited\", \"fieldSeparator\": \""+args3+"\" ,\"tsMaskList\": [\"\"],\"skipFirstRow\": \""+args4+"\",\"fieldPositionList\": []},\"fields\": ["
    
    var json1 ="{\"id\": \""+id+"\",\"objectDefinition\" : {\"lineSeparator\" : \"Line Break\",\"numberOfColumns\" : \""+FC+"\",\"type\" : \"delimited\",\"header\" : \""+args4+"\",\"line\" : {\"quoteChar\" : \"x22\",\"cell\" : ["
    
    //Finding Statistics
    var MeanArray = new Array[String](FC)
    
    var MeanArray1 = new Array[String](FC)
    
    for( i <- 0 until TypeArray.length){
      if(TypeArray(i)=="Numeric"){
        //Numeric Stats
        val results = spark.sql("SELECT ROUND(AVG(_c"+i+"),2) FROM people")//Average
        
        val results1 = spark.sql("SELECT ROUND(STDDEV(_c"+i+"),2) FROM people")//Standard Deviation
        
        val results2 = spark.sql("SELECT ROUND(VARIANCE(_c"+i+"),2) FROM people")//Variance
        
        val results3 = spark.sql("SELECT ROUND(KURTOSIS(_c"+i+"),2) FROM people")//Kurtosis
        
        val results4 = spark.sql("SELECT ROUND(SKEWNESS(_c"+i+"),2) FROM people")//Skewness
        
        val results6 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.25),2) FROM people")//First Quartile
        
        val results7 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.5),2) FROM people")//Median
        
        val results8 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.75),2) FROM people")//Third Quartile
        
        val results10 = spark.sql("SELECT MAX(_c"+i+") FROM people")//Max Value
        
        val results11 = spark.sql("SELECT MIN(_c"+i+") FROM people")//Min Value
        
        val results12 = spark.sql("SELECT Count(*)-Count(_c"+i+") FROM people") //NullCount
        
        val results13 = spark.sql("SELECT MAX(Length(_c"+i+")) FROM people ") //Max Length
        
        val results14 = spark.sql("SELECT MIN(Length(_c"+i+")) FROM people ") //Min Length
        
        val results15 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+") Desc") //Top 10 Values
        
        val results16 = spark.sql("SELECT Count(DISTINCT _c"+i+") FROM people") //Cardinality
        
        val results17 = spark.sql("SELECT ROUND((CAST(Count(_c"+i+") AS float) / CAST(Count(*) AS float))*100,2) From people") //Data Population
        
        val results18 = spark.sql("SELECT SUM(LENGTH(TRIM(TRANSLATE(_c"+i+",' +-0123456789','a ')))) FROM people")
 
        val NumberCheck = results18.head().get(0)
        
        var TypeC = ""
      
        if(NumberCheck==0){
          TypeC= "INTEGER"
          }
         else
          {
        TypeC= "DOUBLE"
          }
        
        var DP = results17.head().getDouble(0).ceil.toString()
        
        var Mean =""
        
        var Mode =""
        
        if(results.head().anyNull==true)
        {
          Mean="NULL"
          Mode="NULL"
        }
        else
        {
          Mean=results.head().getDouble(0).ceil.toString() //Mean
          Mode=results15.head().getString(0) //Mode
        }
          
        var Stddev =""
        
        if(results1.head().anyNull==true)
          Stddev="NULL"
        else
          Stddev=results1.head().getDouble(0).ceil.toString() //Std Deviation
          
        var Variance =""
        
        if(results2.head().anyNull==true)
          Variance="NULL"
        else
          Variance=results2.head().getDouble(0).ceil.toString() //Variance
          
        var Kurtosis =""
        
        if(results3.head().anyNull==true)
          Kurtosis="NULL"
        else
          Kurtosis=results3.head().getDouble(0).ceil.toString() //Kurtosis
          
        var Skewness =""
        
        if(results4.head().anyNull==true)
          Skewness="NULL"
        else
          Skewness=results4.head().getDouble(0).ceil.toString() //Skewness
        
        var Quartile1=""
        var Quartile2=""
        var Quartile3=""
        var IQ=""
        var SemiQuartile=""
        
        if(results6.head().anyNull==true)
        {
          Quartile1="NULL"
          Quartile2="NULL"
          Quartile3="NULL"
          IQ="NULL"
          SemiQuartile="NULL"
        }
        else
        {
          Quartile1=  results6.head().getDouble(0).ceil.toString() //First Quartile
          Quartile2=  results7.head().getDouble(0).ceil.toString() //Second Quartile
          Quartile3=  results8.head().getDouble(0).ceil.toString() //Third Quartile  
          IQ= (Quartile3.toDouble-Quartile1.toDouble).ceil.toString() //InterQuartile
          SemiQuartile= (IQ.toDouble/2).round.toString()
        }
        
        var Max =""
        
        if(results10.head().anyNull==true)
          Max="NULL"
        else
          Max=results10.head().getString(0) //Max Value
          
        var Min =""
        
        if(results11.head().anyNull==true)
          Min="NULL"
        else
          Min=results11.head().getString(0) //Min Value
          
        val x = results15.take(10)
        MeanArray(i)=""
        for(j<- 0 until x.length){
          if(x(j).getString(0)==null||x(j).getLong(1)==null){
            MeanArray(i)= MeanArray(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray(i)= MeanArray(i)+x(j).getString(0)+" ("+x(j).getLong(1)+"),"
          }
        } //Top Values Histogram
        MeanArray(i)= MeanArray(i).dropRight(1) 
        
        var nullCount =""
        
        if(results10.head().anyNull==true)
          nullCount="NULL"
        else
          nullCount=results12.head().getLong(0).toString() //Null Count Value
          
        var histogram =""
        
        if(results16.head().anyNull==true)
          histogram="NULL"
        else
          histogram=results16.head().getLong(0).toString() //Histogram Value
          
        var MaxLength =""
        
        if(results13.head().anyNull==true)
          MaxLength="NULL"
        else
          MaxLength=results13.head().getInt(0).toString() //Max Length Value
          
        var MinLength =""
        
        if(results14.head().anyNull==true)
          MinLength="NULL"
        else
          MinLength=results14.head().getInt(0).toString() //Min Length Value
        
        if(args4==true){
          json=json+ "{\"fieldName\": \""+fieldNames(i).name+"\","
          json1= json1+ "{\"fieldName\" : \""+fieldNames(i).name+"\","
        }else{
          json=json+ "{\"fieldName\": \"_c"+i+"\","
          json1= json1+ "{\"name\" : \"_c"+i+"\","
        }
          
        //Statistics JSON String
        json= json+ "\"fieldType\": \""+TypeC+"\",\"fieldMetricList\": [{\"metricType\": \"Single Value\",\"metricName\": \"Mean\",\"metricData\": [{ \"value\": \""+Mean+"\", \"key\": \"Mean\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Mode\",\"metricData\": [{ \"value\": \""+Mode+"\", \"key\": \"Mode\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Standard Deviation\",\"metricData\": [{ \"value\": \""+Stddev+"\", \"key\": \"Standard Deviation\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Variance\",\"metricData\": [{ \"value\": \""+Variance+"\", \"key\": \"Variance\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Kurtosis\",\"metricData\": [{ \"value\": \""+Kurtosis+"\", \"key\": \"Kurtosis\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Skewness\",\"metricData\": [{ \"value\": \""+Skewness+"\", \"key\": \"Skewness\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Cardinality\",\"metricData\": [{ \"value\": \""+results16.head().getLong(0)+"\", \"key\": \"Cardinality\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Median\",\"metricData\": [{ \"value\": \""+Quartile2+"\", \"key\": \"Median\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"First Quartile\",\"metricData\": [{ \"value\": \""+Quartile1+"\", \"key\": \"First Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Semi Quartile\",\"metricData\": [{ \"value\": \""+SemiQuartile+"\", \"key\": \"Semi Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Third Quartile\",\"metricData\": [{ \"value\": \""+Quartile3+"\", \"key\": \"Third Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max\",\"metricData\": [{ \"value\": \""+Max+"\", \"key\": \"Max\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min\",\"metricData\": [{ \"value\": \""+Min+"\", \"key\": \"Min\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Precision Level\",\"metricData\": [{ \"value\": \""+15+"\", \"key\": \"Precision Level\"}]}"
        
        json= json+ ",{\"metricType\": \"Single Value\",\"metricName\": \"NULL Count\",\"metricData\": [{ \"value\": \""+nullCount+"\", \"key\": \"Count\"}]},{\"metricType\": \"Histogram\",\"metricName\": \"Type Histogram\",\"metricData\": [{ \"value\": \""+histogram+"\", \"key\": \"UNSIGNED NUMERIC\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max Length\",\"metricData\": [{ \"value\": \""+MaxLength+"\", \"key\": \"Max Length\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min Length\",\"metricData\": [{ \"value\": \""+MinLength+"\", \"key\": \"Min Length\"}]}"
        
        json= json+ ",{\"metricType\": \"Single Value\",\"metricName\": \"Inter Quartile\",\"metricData\": [{ \"value\": \""+IQ+"\", \"key\": \"Inter Quartile\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Top 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray(i)+"]\", \"key\": \"Top 10 Occurring Values\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Populated Data\",\"metricData\": [{ \"value\": \""+DP+"\", \"key\": \"Populated Data\"}]}],\"fieldLength\": \""+MaxLength+"\"},"
        
        //MetaData JSON String
        json1= json1+ "\"format\" : {\"type\" : \"Numeric\"},\"origin\" : {\"index\" : \""+i+"\"},\"destinationColumnIndex\" : \""+(i+1)+"\"},"
      
      }
      else if(TypeArray(i)=="String"){
        //String Stats
        val results = spark.sql("SELECT Count(*)-Count(_c"+i+") FROM people") //NullCount
        
        val results1 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+") Desc")
        
        val results2 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+")")
        
        val results3 = spark.sql("SELECT MAX(LENGTH(_c"+i+")) FROM people ") //Max Length
        
        val results4 = spark.sql("SELECT MIN(LENGTH(_c"+i+")) FROM people ") //Min Length
        
        val results5 = spark.sql("SELECT Count(DISTINCT _c"+i+") FROM people") //Cardinality
        
        val results6 = spark.sql("SELECT ROUND((CAST(Count(_c"+i+") AS float) / CAST(Count(*) AS float))*100,2) From people") //Data Population        
        
        var DP= results6.head().getDouble(0).toString()
        
        val x = results1.take(10)
        
        MeanArray(i)=""
        for(j<- 0 until x.length){
          if(x(j).getString(0)==null||x(j).getLong(1)==null){
            MeanArray(i)= MeanArray(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray(i)= MeanArray(i)+x(j).getString(0)+" ("+x(j).getLong(1)+"),"
          }
        }
        MeanArray(i)= MeanArray(i).dropRight(1)//Top 10 Occurring values
        
        val y = results2.take(10)
        
        MeanArray1(i)=""
        for(j<- 0 until y.length){
          if(y(j).getString(0)==null||y(j).getLong(1)==null){
            MeanArray1(i)= MeanArray1(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray1(i)= MeanArray1(i)+y(j).getString(0)+" ("+y(j).getLong(1)+"),"
          }
        }
        MeanArray1(i)=MeanArray1(i).dropRight(1)//Bottom 10 occurring Values
        
        var nullCount =""
        
        if(results.head().anyNull==true)
          nullCount="NULL"
        else
          nullCount=results.head().getLong(0).toString() //Null Count Value
          
        var cardinality =""
        
        if(results5.head().anyNull==true)
          cardinality="NULL"
        else
          cardinality=results5.head().getLong(0).toString() //Cardinality
          
        var histogram =""
        
        if(results5.head().anyNull==true)
          histogram="NULL"
        else
          histogram=results5.head().getLong(0).toString() //Histogram
          
        var MaxLength =""
        
        if(results3.head().anyNull==true)
          MaxLength="NULL"
        else
          MaxLength=results3.head().getInt(0).toString() //Max Length Value
          
        var MinLength =""
        
        if(results4.head().anyNull==true)
          MinLength="NULL"
        else
          MinLength=results4.head().getInt(0).toString() //Min Length Value
          
        if(args4==true){
          json=json+ "{\"fieldName\": \""+fieldNames(i).name+"\","
          json1= json1+ "{\"fieldName\" : \""+fieldNames(i).name+"\","
        }else{
          json=json+ "{\"fieldName\": \"_c"+i+"\","
          json1= json1+ "{\"name\" : \"_c"+i+"\","
        }  
        
        //Statistics JSON String
        json= json+ "\"fieldType\": \"STRING\",\"fieldMetricList\": [{\"metricType\": \"Single Value\",\"metricName\": \"NULL Count\",\"metricData\": [{ \"value\": \""+nullCount+"\", \"key\": \"Count\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Cardinality\",\"metricData\": [{ \"value\": \""+cardinality+"\", \"key\": \"Cardinality\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max Length\",\"metricData\": [{ \"value\": \""+MaxLength+"\", \"key\": \"Max Length\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min Length\",\"metricData\": [{ \"value\": \""+MinLength+"\", \"key\": \"Min Length\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Top 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray(i)+"]\", \"key\": \"Top 10 Occurring Values\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Bottom 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray1(i)+"]\", \"key\": \"Bottom 10 Occurring Values\"}]},{\"metricType\": \"Histogram\",\"metricName\": \"Type Histogram\",\"metricData\": [{ \"value\": \""+histogram+"\", \"key\": \"UNSIGNED NUMERIC\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Populated Data\",\"metricData\": [{ \"value\": \""+DP+"\", \"key\": \"Populated Data\"}]}],\"fieldLength\":\""+MaxLength+"\"},"
      
        //MetaData JSON String
        json1= json1+ "\"format\" : {\"type\" : \"String\"},\"origin\" : {\"index\" : \""+i+"\"},\"destinationColumnIndex\" : \""+(i+1)+"\"},"
      }
    }
    //TimeStamp
    val timestamp1= Calendar.getInstance.getTime
    
    val dateFormat1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    
    val after= dateFormat1.format(timestamp1)
    
    json= json.dropRight(1)
    
    json=json+"],\"info\": {\"profilerStarted\": \""+before+"\",\"profileUpdated\": \""+after+"\",\"objectName\": \""+objectName+"\",\"lastProfilerMsg\": \"Available\",\"numberOfRows\":\""+TotalRecords+"\",\"profilerFinished\": \""+after+"\"}}"
    
    json1= json1.dropRight(1)
    
    json1= json1+ "],\"sourceCellSeparator\" : \"|\"}},\"info\" : {\"objectName\" : \""+objectName+"\",\"objectType\" : \"Delimited\",\"user\" : \"\",\"created\" : \""+before+"\",\"updated\" : \""+after+"\",\"status\" : \"0\"}}"
    
    //Print Statements
    //spark.sparkContext.parallelize(Seq(json)).repartition(1).saveAsTextFile(args1)
    spark.sparkContext.parallelize(Seq(json)).repartition(1).saveJsonToEs("xdf_data_profiler/dataProfiler", Map("es.mapping.id" -> "id"))
    //spark.sparkContext.parallelize(Seq(json1)).repartition(1).saveAsTextFile(args2)
    //spark.sparkContext.parallelize(Seq(json1)).repartition(1).saveJsonToEs("xdf_meta_data/metaData", Map("es.mapping.id" -> "id"))
 }
  
  def ReadParquet(spark: SparkSession, id:String, args:String, header:Boolean, objectName:String): Unit = {
    
    import spark.implicits._ 
    
    val df = spark.read.option("header", header).parquet(args)
    val fieldNames = df.schema.fields
    
    //Converting RDD into an array for Traversal
    val FirstRow = df.first()
    val FC= FirstRow.length
    
    //Renaming fields for Ease of Iteration
    var newNames = Seq("_c0")
    for(i <- 1 until FC)
    newNames =  newNames :+ "_c"+i
    val dfRenamed = df.toDF(newNames: _*)
   
    //Creating table for Querying 
    dfRenamed.createOrReplaceTempView("people")
    
    // Counting Total records in the RDD
    val TotalRecords = df.count() 
    
    var TypeArray = new Array[String](FC)
    
    //Checking for Field Type
    for( i <- 0 until FC ){    
      val results = spark.sql("SELECT SUM(LENGTH(TRIM(TRANSLATE(_c"+i+",' +-.0123456789','a ')))) FROM people")
      
      val FinalResults = results.head().getLong(0)
      
      if(FinalResults==0){
        TypeArray(i)= "Numeric"
      }
      else
      {
        TypeArray(i)= "String"
      }
    }
    
    //TimeStamp
    val timestamp= Calendar.getInstance.getTime
    
    val dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    
    val before= dateFormat.format(timestamp)
    
    var json= "{\"id\": \""+id+"\",\"parameters\": {\"location\": \""+args+"\",\"type\": \"columnar\", \"fieldSeparator\": \"parquet\" ,\"tsMaskList\": [\"\"],\"skipFirstRow\": \""+header+"\",\"fieldPositionList\": []},\"fields\": ["
    
    var json1 ="{\"id\": \""+id+"\",\"objectDefinition\" : {\"lineSeparator\" : \"Line Break\",\"numberOfColumns\" : \""+FC+"\",\"type\" : \"delimited\",\"header\" : \""+header+"\",\"line\" : {\"quoteChar\" : \"x22\",\"cell\" : ["
    
    //Finding Statistics
    var MeanArray = new Array[String](FC)
    
    var MeanArray1 = new Array[String](FC)
    
    for( i <- 0 until TypeArray.length){
      if(TypeArray(i)=="Numeric"){
        //Numeric Stats
        val results = spark.sql("SELECT ROUND(AVG(_c"+i+"),2) FROM people")
        
        val results1 = spark.sql("SELECT ROUND(STDDEV(_c"+i+"),2) FROM people")
        
        val results2 = spark.sql("SELECT ROUND(VARIANCE(_c"+i+"),2) FROM people")
        
        val results3 = spark.sql("SELECT ROUND(KURTOSIS(_c"+i+"),2) FROM people")
        
        val results4 = spark.sql("SELECT ROUND(SKEWNESS(_c"+i+"),2) FROM people")
        
        val results6 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.25),2) FROM people")
        
        val results7 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.5),2) FROM people")
        
        val results8 = spark.sql("SELECT ROUND(PERCENTILE(_c"+i+", 0.75),2) FROM people")
        
        val results10 = spark.sql("SELECT MAX(_c"+i+") FROM people")
        
        val results11 = spark.sql("SELECT MIN(_c"+i+") FROM people")
        
        val results12 = spark.sql("SELECT Count(*)-Count(_c"+i+") FROM people") //NullCount
        
        val results13 = spark.sql("SELECT MAX(Length(_c"+i+")) FROM people ") //Max Length
        
        val results14 = spark.sql("SELECT MIN(Length(_c"+i+")) FROM people ") //Min Length
        
        val results15 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+") Desc") //Top 10 Values
        
        val results16 = spark.sql("SELECT Count(DISTINCT _c"+i+") FROM people") //Cardinality
        
        val results17 = spark.sql("SELECT ROUND((CAST(Count(_c"+i+") AS float) / CAST(Count(*) AS float))*100,2) From people") //Data Population
        
        var DP = results17.head().getDouble(0).ceil.toString()
        
        var Mean =""
        
        var Mode =""
        
        if(results.head().anyNull==true)
        {
          Mean="NULL"
          Mode="NULL"
        }
        else
        {
          Mean=results.head().getDouble(0).ceil.toString() //Mean
          Mode=results15.head().getString(0) //Mode
        }
          
        var Stddev =""
        
        if(results1.head().anyNull==true)
          Stddev="NULL"
        else
          Stddev=results1.head().getDouble(0).ceil.toString() //Std Deviation
          
        var Variance =""
        
        if(results2.head().anyNull==true)
          Variance="NULL"
        else
          Variance=results2.head().getDouble(0).ceil.toString() //Variance
          
        var Kurtosis =""
        
        if(results3.head().anyNull==true)
          Kurtosis="NULL"
        else
          Kurtosis=results3.head().getDouble(0).ceil.toString() //Kurtosis
          
        var Skewness =""
        
        if(results4.head().anyNull==true)
          Skewness="NULL"
        else
          Skewness=results4.head().getDouble(0).ceil.toString() //Skewness
        
        var Quartile1=""
        var Quartile2=""
        var Quartile3=""
        var IQ=""
        var SemiQuartile=""
        
        if(results6.head().anyNull==true)
        {
          Quartile1="NULL"
          Quartile2="NULL"
          Quartile3="NULL"
          IQ="NULL"
          SemiQuartile="NULL"
        }
        else
        {
          Quartile1=  results6.head().getDouble(0).ceil.toString() //First Quartile
          Quartile2=  results7.head().getDouble(0).ceil.toString() //Second Quartile
          Quartile3=  results8.head().getDouble(0).ceil.toString() //Third Quartile  
          IQ= (Quartile3.toDouble-Quartile1.toDouble).ceil.toString() //InterQuartile
          SemiQuartile= (IQ.toDouble/2).ceil.toString()
        }
        
        var Max =""
        
        if(results10.head().anyNull==true)
          Max="NULL"
        else
          Max=results10.head().getString(0) //Max Value
          
        var Min =""
        
        if(results11.head().anyNull==true)
          Min="NULL"
        else
          Min=results11.head().getString(0) //Min Value
          
        val x = results15.take(10)
        MeanArray(i)=""
        for(j<- 0 until x.length){
          if(x(j).getString(0)==null||x(j).getLong(1)==null){
            MeanArray(i)= MeanArray(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray(i)= MeanArray(i)+x(j).getString(0)+" ("+x(j).getLong(1)+"),"
          }
        } //Top Values Histogram
        MeanArray(i)= MeanArray(i).dropRight(1) 
        
        var nullCount =""
        
        if(results10.head().anyNull==true)
          nullCount="NULL"
        else
          nullCount=results12.head().getLong(0).toString() //Null Count Value
          
        var histogram =""
        
        if(results16.head().anyNull==true)
          histogram="NULL"
        else
          histogram=results16.head().getLong(0).toString() //Histogram Value
          
        var MaxLength =""
        
        if(results13.head().anyNull==true)
          MaxLength="NULL"
        else
          MaxLength=results13.head().getInt(0).toString() //Max Length Value
          
        var MinLength =""
        
        if(results14.head().anyNull==true)
          MinLength="NULL"
        else
          MinLength=results14.head().getInt(0).toString() //Min Length Value
        
        if(header==true){
          json=json+ "{\"fieldName\": \""+fieldNames(i).name+"\","
          json1= json1+ "{\"fieldName\" : \""+fieldNames(i).name+"\","
        }else{
          json=json+ "{\"fieldName\": \"_c"+i+"\","
          json1= json1+ "{\"name\" : \"_c"+i+"\","
        }
          
        //Statistics JSON String
        json= json+ "\"fieldType\": \""+"Numeric"+"\",\"fieldMetricList\": [{\"metricType\": \"Single Value\",\"metricName\": \"Mean\",\"metricData\": [{ \"value\": \""+Mean+"\", \"key\": \"Mean\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Mode\",\"metricData\": [{ \"value\": \""+Mode+"\", \"key\": \"Mode\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Standard Deviation\",\"metricData\": [{ \"value\": \""+Stddev+"\", \"key\": \"Standard Deviation\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Variance\",\"metricData\": [{ \"value\": \""+Variance+"\", \"key\": \"Variance\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Kurtosis\",\"metricData\": [{ \"value\": \""+Kurtosis+"\", \"key\": \"Kurtosis\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Skewness\",\"metricData\": [{ \"value\": \""+Skewness+"\", \"key\": \"Skewness\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Cardinality\",\"metricData\": [{ \"value\": \""+results16.head().getLong(0)+"\", \"key\": \"Cardinality\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Median\",\"metricData\": [{ \"value\": \""+Quartile2+"\", \"key\": \"Median\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"First Quartile\",\"metricData\": [{ \"value\": \""+Quartile1+"\", \"key\": \"First Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Semi Quartile\",\"metricData\": [{ \"value\": \""+SemiQuartile+"\", \"key\": \"Semi Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Third Quartile\",\"metricData\": [{ \"value\": \""+Quartile3+"\", \"key\": \"Third Quartile\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max\",\"metricData\": [{ \"value\": \""+Max+"\", \"key\": \"Max\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min\",\"metricData\": [{ \"value\": \""+Min+"\", \"key\": \"Min\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Precision Level\",\"metricData\": [{ \"value\": \""+15+"\", \"key\": \"Precision Level\"}]}"
        
        json= json+ ",{\"metricType\": \"Single Value\",\"metricName\": \"NULL Count\",\"metricData\": [{ \"value\": \""+nullCount+"\", \"key\": \"Count\"}]},{\"metricType\": \"Histogram\",\"metricName\": \"Type Histogram\",\"metricData\": [{ \"value\": \""+histogram+"\", \"key\": \"UNSIGNED NUMERIC\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max Length\",\"metricData\": [{ \"value\": \""+MaxLength+"\", \"key\": \"Max Length\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min Length\",\"metricData\": [{ \"value\": \""+MinLength+"\", \"key\": \"Min Length\"}]}"
        
        json= json+ ",{\"metricType\": \"Single Value\",\"metricName\": \"Inter Quartile\",\"metricData\": [{ \"value\": \""+IQ+"\", \"key\": \"Inter Quartile\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Top 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray(i)+"]\", \"key\": \"Top 10 Occurring Values\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Populated Data\",\"metricData\": [{ \"value\": \""+DP+"\", \"key\": \"Populated Data\"}]}],\"fieldLength\": \""+MaxLength+"\"},"
        
        //MetaData JSON String
        json1= json1+ "\"format\" : {\"type\" : \"Numeric\"},\"origin\" : {\"index\" : \""+i+"\"},\"destinationColumnIndex\" : \""+(i+1)+"\"},"
      
      }
      else if(TypeArray(i)=="String"){
        //String Stats
        
        val results = spark.sql("SELECT Count(*)-Count(_c"+i+") FROM people") //NullCount
        
        val results1 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+") Desc")
        
        val results2 = spark.sql("SELECT _c"+i+", Count(_c"+i+") FROM people GROUP BY _c"+i+" Order by Count(_c"+i+")")
        
        val results3 = spark.sql("SELECT MAX(Length(_c"+i+")) FROM people ") //Max Length
        
        val results4 = spark.sql("SELECT MIN(Length(_c"+i+")) FROM people ") //Min Length
        
        val results5 = spark.sql("SELECT Count(DISTINCT _c"+i+") FROM people") //Cardinality
        
        val results6 = spark.sql("SELECT ROUND((CAST(Count(_c"+i+") AS float) / CAST(Count(*) AS float))*100,2) From people") //Data Population        
        
        var DP= results6.head().getDouble(0).toString()
        
        val x = results1.take(10)
        
        MeanArray(i)=""
        for(j<- 0 until x.length){
          if(x(j).getString(0)==null||x(j).getLong(1)==null){
            MeanArray(i)= MeanArray(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray(i)= MeanArray(i)+x(j).getString(0)+" ("+x(j).getLong(1)+"),"
          }
        }
        MeanArray(i)= MeanArray(i).dropRight(1)//Top 10 Occurring values
        
        val y = results2.take(10)
        
        MeanArray1(i)=""
        for(j<- 0 until y.length){
          if(y(j).getString(0)==null||y(j).getLong(1)==null){
            MeanArray1(i)= MeanArray1(i)+"NULL"+" ("+"NULL"+"),"
          }else
          {
          MeanArray1(i)= MeanArray1(i)+y(j).getString(0)+" ("+y(j).getLong(1)+"),"
          }
        }
        MeanArray1(i)=MeanArray1(i).dropRight(1)//Bottom 10 occurring Values
        
        var nullCount =""
        
        if(results.head().anyNull==true)
          nullCount="NULL"
        else
          nullCount=results.head().getLong(0).toString() //Null Count Value
          
        var cardinality =""
        
        if(results5.head().anyNull==true)
          cardinality="NULL"
        else
          cardinality=results5.head().getLong(0).toString() //Cardinality
          
        var histogram =""
        
        if(results5.head().anyNull==true)
          histogram="NULL"
        else
          histogram=results5.head().getLong(0).toString() //Histogram
          
        var MaxLength =""
        
        if(results3.head().anyNull==true)
          MaxLength="NULL"
        else
          MaxLength=results3.head().getInt(0).toString() //Max Length Value
          
        var MinLength =""
        
        if(results4.head().anyNull==true)
          MinLength="NULL"
        else
          MinLength=results4.head().getInt(0).toString() //Min Length Value
          
        if(header==true){
          json=json+ "{\"fieldName\": \""+fieldNames(i).name+"\","
          json1= json1+ "{\"fieldName\" : \""+fieldNames(i).name+"\","
        }else{
          json=json+ "{\"fieldName\": \"_c"+i+"\","
          json1= json1+ "{\"name\" : \"_c"+i+"\","
        }  
        
        //Statistics JSON String
        json= json+ "\"fieldType\": \"String\",\"fieldMetricList\": [{\"metricType\": \"Single Value\",\"metricName\": \"NULL Count\",\"metricData\": [{ \"value\": \""+nullCount+"\", \"key\": \"Count\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Cardinality\",\"metricData\": [{ \"value\": \""+cardinality+"\", \"key\": \"Cardinality\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Max Length\",\"metricData\": [{ \"value\": \""+MaxLength+"\", \"key\": \"Max Length\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Min Length\",\"metricData\": [{ \"value\": \""+MinLength+"\", \"key\": \"Min Length\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Top 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray(i)+"]\", \"key\": \"Top 10 Occurring Values\"}]},{\"metricType\": \"Multiple Values\",\"metricName\": \"Bottom 10 Occurring Values\",\"metricData\": [{ \"value\": \"["+MeanArray1(i)+"]\", \"key\": \"Bottom 10 Occurring Values\"}]},{\"metricType\": \"Histogram\",\"metricName\": \"Type Histogram\",\"metricData\": [{ \"value\": \""+histogram+"\", \"key\": \"UNSIGNED NUMERIC\"}]},{\"metricType\": \"Single Value\",\"metricName\": \"Populated Data\",\"metricData\": [{ \"value\": \""+DP+"\", \"key\": \"Populated Data\"}]}],\"fieldLength\":\""+MaxLength+"\"},"
      
        //MetaData JSON String
        json1= json1+ "\"format\" : {\"type\" : \"String\"},\"origin\" : {\"index\" : \""+i+"\"},\"destinationColumnIndex\" : \""+(i+1)+"\"},"
      }
    }
    //TimeStamp
    val timestamp1= Calendar.getInstance.getTime
    
    val dateFormat1 = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss")
    
    val after= dateFormat1.format(timestamp1)
    
    json= json.dropRight(1)
    
    json=json+"],\"info\": {\"profilerStarted\": \""+before+"\",\"profileUpdated\": \""+after+"\",\"objectName\": \""+objectName+"\",\"lastProfilerMsg\": \"Available\",\"numberOfRows\":\""+TotalRecords+"\",\"profilerFinished\": \""+after+"\"}}"
    
    json1= json1.dropRight(1)
    
    json1= json1+ "],\"sourceCellSeparator\" : \"|\"}},\"info\" : {\"objectName\" : \""+objectName+"\",\"objectType\" : \"Delimited\",\"user\" : \"\",\"created\" : \""+before+"\",\"updated\" : \""+after+"\",\"status\" : \"0\"}}"
    
    //Print Statements
    //spark.sparkContext.parallelize(Seq(json)).repartition(1).saveAsTextFile(args1)
    spark.sparkContext.parallelize(Seq(json)).repartition(1).saveJsonToEs("xdf_data_profiler/dataProfiler", Map("es.mapping.id" -> "id"))
    //spark.sparkContext.parallelize(Seq(json1)).repartition(1).saveAsTextFile(args2)
    //spark.sparkContext.parallelize(Seq(json1)).repartition(1).saveJsonToEs("xdf_meta_data/metaData", Map("es.mapping.id" -> "id"))
 
  }
  
  def main(args: Array[String]): Unit = {
    //Spark Configuration settings
    //val conf = new SparkConf()
    //.setMaster("spark://10.48.22.173:7077")
      
    //val sc = new SparkContext()
    //sc.getConf.get("")
    
    val spark = SparkSession.builder().
    appName("Q1").config("spark.sql.warehouse.dir", "/tmp/spark-warehouse/").getOrCreate()
    //spark.conf.set("spark.executor.memory", "1g")
    //spark.conf.set("spark.cores.max", "1")
    //spark.conf.set("spark.driver.memory", "1g")

    spark.sparkContext.getConf.get("spark.executor.memory")
    spark.sparkContext.getConf.get("spark.driver.memory")
    spark.sparkContext.getConf.get("spark.cores.max")
    spark.sparkContext.getConf.get("spark.logConf")
    spark.sparkContext.getConf.get("spark.history.fs.logDirectory")
    spark.sparkContext.getConf.get("spark.eventLog.enabled")
    spark.sparkContext.getConf.get("spark.eventLog.dir")
    spark.sparkContext.getConf.get("spark.master")
    spark.sparkContext.getConf.get("spark.es.nodes")
    spark.sparkContext.getConf.get("spark.es.admin.port")
    spark.sparkContext.getConf.get("spark.es.cluster.name")
    spark.sparkContext.getConf.get("spark.es.net.http.auth.user")
    spark.sparkContext.getConf.get("spark.es.net.http.auth.pass")
    spark.sparkContext.getConf.get("spark.es.write.operation")
    spark.sparkContext.getConf.get("spark.es.mapping.id")
        
    val a=spark.sparkContext.getConf.getAll
    
    a.foreach(a=>println(a))
    val id= args(0)
    
    val parameterRDD = spark.sparkContext.esJsonRDD("xdf_data_profiler/dataProfiler","?q="+id+"").values
    
    val df = spark.sqlContext.read.json(parameterRDD)
    df.createOrReplaceTempView("ESparameters")
    val parameters= spark.sql("select parameters.location, parameters.type, parameters.fieldSeparator, parameters.skipFirstRow, info.objectName from ESparameters")
    val location = parameters.head().getString(0)
    val fileType = parameters.head().getString(1)
    val delimiter = parameters.head().getString(2)
    val header = parameters.head().getBoolean(3)
    val objectName = parameters.head().getString(4)
    
    //Calling Functions
    if(fileType=="delimited"||fileType=="Delimited"){
    
      ReadCSV(spark,id,location, delimiter, header, objectName)
    }
    else if(fileType=="parquet"){
    
     ReadParquet(spark,id,location, header, objectName)
    }
  }
}