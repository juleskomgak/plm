package com.renault.plmdashboarding

import org.apache.spark.sql.functions._
import org.apache.spark.SparkConf
import org.apache.spark.SparkContext
import org.apache.spark.SparkContext._
import org.elasticsearch.spark._
import org.apache.spark.sql.hive.orc._
import org.apache.spark.sql._
import org.apache.spark.sql.functions._

import scala.util.control.Breaks._
import org.apache.spark.sql.SQLContext    
import org.apache.spark.sql.SQLContext._

import org.elasticsearch.spark.sql._ 

import scala.collection.mutable.ListBuffer
import scala.collection.mutable.Map


/**
 * @author ${user.name}
 */
object App2 {

	def main(args: Array[String]): Unit = {

			val conf = new SparkConf()
			.setAppName("appName")
			.setMaster("local[*]")
					

					val sc = new SparkContext(conf)

					val sqlContext = new SQLContext(sc)
					val hiveContext   = new org.apache.spark.sql.hive.HiveContext(sc)



					hiveContext.setConf("spark.sql.orc.filterPushdown", "true") 
					hiveContext.setConf("spark.sql.caseSensitive", "true");


			//filter(inputDF("Ref_E_GFE").isNull || inputDF("Ref_E_GFE") === "" || inputDF("Ref_E_GFE").isNaN)*/


			//val inputDF = hiveContext.sql("select * from db_raw_irn_53501_npd.npdm_last_histo") 
			
			
			    
		    val inputDF = hiveContext.read
      .format("com.databricks.spark.csv").option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/user/Documents/DOC_CONSULTANT/Doc_projet_PLM/doc_datalake/dataLake19072017.csv")
			
			
      println("*************************")
			println("*************************")
			println("*************************")
			println("Nbre de ligne en entrée : "+inputDF.count())
			
			println("*************************")
			println("*************************")
			
					val inputReduceDF = inputDF.select(
							inputDF("physical_instance_path"),
							inputDF("ref_id"),
							inputDF("refinst_e_id_physique_aplt"),
							inputDF("filter_name"),
							inputDF("ref_e_ref_externe"),
							inputDF("ref_e_description_uk"),
							inputDF("ref_e_name"),
							inputDF("refinst_id"),
							inputDF("refinst_e_gen_prt_cod"),
							inputDF("refinst_e_proposed"),
							inputDF("refinst_e_inheritedconfig"),
							inputDF("refinst_e_localconfig"),
							inputDF("refinst_e_arch_zone"),
							inputDF("ref_revisionindex"),
							inputDF("ref_e_sts_s"),
							inputDF("ref_maturity"),
							inputDF("refinst_e_gel"),
							inputDF("refinst_timemodified"),
							inputDF("ref_v_custodiscipline"),
							inputDF("ref_e_gfe"),
							inputDF("refinst_e_parenttype"))
							.dropDuplicates(Array("physical_instance_path"))















					// delete all null  Physical_Instance_Path column 

					val inputReduceDF_notnull_phy_path=inputReduceDF.filter(inputReduceDF("physical_instance_path").isNotNull)
					

					val oci_ind_filter= inputReduceDF_notnull_phy_path.filter(((
							inputReduceDF("ref_v_custodiscipline")==="OCI")
							||
							(inputReduceDF("ref_v_custodiscipline") ==="IND"))	
							&& 
							(((inputReduceDF("refinst_e_parenttype").notEqual("OCI")) &&  (inputReduceDF("refinst_e_parenttype").notEqual("IND")))))
					.filter("refinst_e_parenttype != ''")
					.filter("ref_id !='x'")





					oci_ind_filter.printSchema()

					oci_ind_filter.show(false)


println("*************************")
			println("*************************")
			println("*************************")
			println("Nbre de ligne OCI IND en entrée : "+oci_ind_filter.count())
			
			println("*************************")
			println("*************************")

					def calculGFEDf1df2(df1: DataFrame , df2: DataFrame) : ListBuffer[String] ={  



							var listGfe =new ListBuffer[String]()
									var  gfe=""

									var i = 0;
							var n = 0;

							val test=df2.select(df2("physical_instance_path")).collect.map({ case Row(c: String) => c })
									.map(line => line.split("/"))



									for (n <- 0 to test.length-1){

										breakable {


											var liste=test(n).reverse.drop(1) 

													for (i <- 0 to liste.length-1){

														import sqlContext.implicits._ 
														

                            if( df1.filter($"ref_id" === liste(i).substring(0,10)).count()>=1){
														var er = df1.filter($"ref_id" === liste(i).substring(0,10)).select("ref_e_gfe")
														.map({ case Row(c: String) => c }).first



														breakable { if (testRegex.textRegCheck(er).equals("ko"))
															break
														} 

														if(testRegex.textRegCheck(er).equals("ok")) {


															listGfe += er

																	break
														}

														if (i==(liste.length-1) && testRegex.textRegCheck(er).equals("ko")){


															listGfe += "0"
														}

													}
                            else {
                              
                              listGfe += "0"
                              
                            }

										}

									}
									}
							listGfe 
									
			}




    
					def calculGFEDf1df2biss(df1: DataFrame , df2: DataFrame) : ListBuffer[String] ={  



							var listGfe =new ListBuffer[String]()
									var  gfe=""

									var i = 0;
							var n = 0;

							val test=df2.select(df2("physical_instance_path")).collect.map({ case Row(c: String) => c })
									.map(line => line.split("/"))



									for (n <- 0 to test.length-1){

										breakable {


											var liste=test(n).reverse.drop(1) 

													for (i <- 0 to liste.length-1){

														import sqlContext.implicits._ 
														

                            if( df1.filter($"ref_id" === liste(i).substring(0,10)).count()>=1){
														var er = df1.filter($"ref_id" === liste(i).substring(0,10)).select("ref_e_gfe")
														.map({ case Row(c: String) => c }).first



														breakable { if (testRegex.textRegCheck(er).equals("ko"))
															break
														} 

														if(testRegex.textRegCheck(er).equals("ok")) {


															listGfe += er

																	break
														}

														if (i==(liste.length-1) && testRegex.textRegCheck(er).equals("ko")){


															listGfe += "0"
														}

													}
                            else {
                              
                              listGfe += "00"
                              
                            }

										}

									}
									}
							listGfe 
									
			}


    // putting the 2 dataFrame in the cache (inputReduceDF_notnull_phy_path  and  oci_ind_filter)
  


			val persist_inputReduceDF_notnull_phy_path=inputReduceDF_notnull_phy_path.cache()
			
			val persist_oci_ind_filter=oci_ind_filter.cache()

 // using the calculGFEDf1df2 function in order to generate a list of all gfe of our dataSet
			
			//val value=calculGFEDf1df2(persist_inputReduceDF_notnull_phy_path,persist_oci_ind_filter).map(_.toInt).toList

       val value=calculGFEDf1df2biss(persist_inputReduceDF_notnull_phy_path,persist_oci_ind_filter).map(_.toInt).toList
     
 
       // create an index for all gfe of the dataSet  
					import sqlContext.implicits._
					val rddList2 = sc.parallelize(value).zipWithIndex().toDF("GFE","lineNumber1")
					
					
		  println("*************************")
			println("*************************")
			println("*************************")
			println("Nbre de ligne FGE en entrée : "+rddList2.count())
			
			println("*************************")
			println("*************************")
			
					
					
     
					rddList2.show(false)
    //println("list number"+rddList2.count)

// test to save the rddList2 in the cluster to test



 

// create an index of any Physical_Instance_Path in our dataSet in order to join with the gfe index

val rdd_LOGIQUE_APLT = persist_oci_ind_filter.select("physical_instance_path")
.rdd.map({ case Row(c: String) => c })
.zipWithIndex()
.toDF("Physical_Instance_Path_join","lineNumber2")

//rdd_LOGIQUE_APLT.show(false)


     println("*************************")
			println("*************************")
			println("*************************")
			println("Nbre de ligne physycal avant jointure en entrée : "+rdd_LOGIQUE_APLT.count())
			
			println("*************************")
			println("*************************")
			

// join of two dataSet 
val join_LOGIQUE_APLT_gfe=persist_oci_ind_filter.join(rdd_LOGIQUE_APLT, rdd_LOGIQUE_APLT("Physical_Instance_Path_join") === persist_oci_ind_filter("physical_instance_path"))


println("list number after join  Physical_Instance_Path_join and physical_instance_path  "+join_LOGIQUE_APLT_gfe.count)

val join_LOGIQUE_APLT_table_gfe=join_LOGIQUE_APLT_gfe.join(rddList2 , join_LOGIQUE_APLT_gfe("lineNumber2") === rddList2("lineNumber1"))

//join_LOGIQUE_APLT_table_gfe.show()

// just to test the needed number of line
 
println("list number after new join :table oci ind "+join_LOGIQUE_APLT_table_gfe.count)


  
    
    
// drop  column   
 
 val oci_ind_filter_join_map=join_LOGIQUE_APLT_table_gfe
 .drop("lineNumber2")
 .drop("lineNumber1")
 .drop("Physical_Instance_Path_join")
 .drop("ref_e_gfe")
 .drop("ref_e_gfe")
 .drop("filter_name")
 
 
 
 
 
   
    // creation of new column oci_ind and tag of  oci and ind   
    

    
		 val tag_oci_ind2 = oci_ind_filter_join_map.withColumn("oci_ind", 
							when(oci_ind_filter_join_map("ref_v_custodiscipline")==="OCI"
							&& length(oci_ind_filter_join_map.col("ref_e_ref_externe")) <10, 0)
							.when(oci_ind_filter_join_map("ref_v_custodiscipline")==="OCI" 
							&& length(oci_ind_filter_join_map.col("ref_e_ref_externe")) >=10, 1)
							.when(oci_ind_filter_join_map("ref_v_custodiscipline")==="IND" 
							, 2)
							.otherwise(3))
							
							
		//test of the tag
							
					//tag_oci_ind2.show()
					
					
					
	// creation of oci_ind table with the requirement (column) of the front End 				

val table_oci_ind_front=tag_oci_ind2
	.withColumnRenamed("physical_instance_path", "id")
	.withColumnRenamed("ref_id", "reference")
	.withColumnRenamed("ref_e_ref_externe", "extReference")
	.withColumnRenamed("ref_e_description_uk", "enDesignation")
	.withColumnRenamed("ref_e_name", "frDesignation")
	.withColumnRenamed("oci_ind", "ociInd")
	.withColumnRenamed("refinst_id", "instance")
	.withColumnRenamed("refinst_e_gen_prt_cod", "PG")
	.withColumnRenamed("refinst_e_proposed", "milestone")
	
	.withColumnRenamed("refinst_e_inheritedconfig", "inheritedConfiguration")
	.withColumnRenamed("refinst_e_localconfig", "localConfiguration")
	.withColumnRenamed("refinst_e_arch_zone", "architectureZone")
	.withColumnRenamed("ref_revisionindex", "version")
	.withColumnRenamed("ref_e_sts_s", "partStatus")
	.withColumnRenamed("ref_maturity","maturity")
	.withColumnRenamed("refinst_e_gel", "gel")
	.withColumnRenamed("refinst_timemodified", "lastUpdate")
	
	
	
	
	table_oci_ind_front.printSchema()
	
	table_oci_ind_front.show()
import sqlContext.implicits._ 
	
val table_oci_ind_front_reduce_rdd=table_oci_ind_front.select(	
	"id",
	"reference",
	"refinst_e_id_physique_aplt",
	"extReference",
  "enDesignation",
  "frDesignation",
  "instance",
  "PG",
  "milestone",
  "inheritedConfiguration",
  "localConfiguration",
  "architectureZone",
  "version",
  "partStatus",
  "maturity",
  "gel",
  "lastUpdate",
  "ref_v_custodiscipline",
  "refinst_e_parenttype",
  "GFE",
  "ociInd"
  ).rdd
   .map(f=>((
          f(0).toString()),
          f(1).toString(),
          f(2).toString(),
          f(3).toString(),
          f(4).toString(),
          f(5).toString(),
          f(6).toString(),
          f(7).toString(),
          f(8).toString(),
          f(9).toString().replaceAll("=== CONFIGURATION ===", ""),
          f(10).toString().replaceAll("=== CONFIGURATION ===", ""),
          f(11).toString(),
          f(12).toString(),
          f(13).toString(),
          f(14).toString(),
          f(15).toString(),
          f(16).toString(),
          f(17).toString(),
          f(18).toString(),
          f(19).toString(),
          f(20).toString()
          ))
     
          val table_oci_ind_front_reduce_post = hiveContext.createDataFrame(table_oci_ind_front_reduce_rdd)
         .withColumnRenamed("_1", "id")
          .withColumnRenamed("_2", "reference")
          .withColumnRenamed("_3", "refinstEIidPhysiqueAplt")
          .withColumnRenamed("_4", "extReference")
          .withColumnRenamed("_5", "enDesignation")
          .withColumnRenamed("_6", "frDesignation")
          .withColumnRenamed("_7", "instance")
          .withColumnRenamed("_8", "PG")
          .withColumnRenamed("_9", "milestone")
          .withColumnRenamed("_10", "inheritedConfiguration")
          .withColumnRenamed("_11", "localConfiguration")
          .withColumnRenamed("_12", "architectureZone")
          .withColumnRenamed("_13", "version")
          .withColumnRenamed("_14", "partStatus")
          .withColumnRenamed("_15", "maturity")
          .withColumnRenamed("_16", "gel")
          .withColumnRenamed("_17", "lastUpdate")
          .withColumnRenamed("_18", "refVCustodiscipline")
          .withColumnRenamed("_19", "refinstEParenttype")
          .withColumnRenamed("_20", "GFE")
          .withColumnRenamed("_21", "ociInd")
          .withColumn("processingDate",current_timestamp.cast("String"))
          
          
    	/*import org.apache.spark.sql.functions.unix_timestamp

		val process_date = unix_timestamp($"processing_date","MM/dd/yyyy HH:mm:ss")
		.cast("String")                                       
	  val lastUp_date = unix_timestamp($"last_update","MM/dd/yyyy HH:mm:ss")
	  .cast("String")
	
	   val table_oci_ind_front_reduce=table_oci_ind_front_reduce_post
		                       .withColumn("processingDate", process_date)
		                       .withColumn("lastUpdate", lastUp_date)
		                       .drop($"processing_date")
		                       .drop($"last_update")*/
	  
	  table_oci_ind_front_reduce_post.printSchema()
	  table_oci_ind_front_reduce_post.show(false)
	
	
	
	
	
	/*
	
	 // save the oci_ind table in one in ORC format in one partition in order to index it in ELK
	
	 
    
    table_oci_ind_front_reduce.coalesce(1)
      .write.mode(SaveMode.Overwrite)
      .orc("hdfs://bigopeclu/data/refinery/irn_67133_d4b/work/oci_ind/")
	
	
	
	// creation of the hive table
    //path of location hive table in the datalake
	
   
   
    
    //val skipHeader  = " TBLPROPERTIES ('skip.header.line.count'='1')"
    val path        = "'hdfs://bigopeclu/data/refinery/irn_67133_d4b/work/oci_ind/'"

    val fieldsAndTypes = "id STRING,reference STRING,extReference STRING,enDesignation STRING,frDesignation STRING,instance STRING,PG STRING,"+
      "milestone STRING,inheritedConfiguration STRING,localConfiguration STRING,architectureZone STRING,version STRING,partStatus STRING,"+
      "maturity STRING,gel STRING,lastUpdate TIMESTAMP,Ref_V_CustoDiscipline STRING,RefInst_E_ParentType STRING,Physical_Instance_Path STRING,GFE STRING,Oci_Ind STRING,Processing_Times TIMESTAMP"
      
    val request : String = "CREATE EXTERNAL TABLE IF NOT EXISTS db_work_irn_67133_d4b.plmd_oci_ind (" + fieldsAndTypes + ")" +
      """  
                               STORED AS ORC  
                             LOCATION """ + path //+ skipHeader
          hiveContext.sql(request)

    

   
    
      
      
// index the dataFrame in ELK

EsSparkSQL.saveToEs(table_oci_ind_front_reduce, "d4b_dev_oci_ind/1")



*/


	}
}