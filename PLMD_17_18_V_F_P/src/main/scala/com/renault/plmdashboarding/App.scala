package com.renault.plmdashboarding

import org.apache.spark.SparkContext
import org.apache.spark.SparkConf
import org.apache.spark.sql.SQLContext
import org.apache.spark.sql.DataFrame
import org.elasticsearch.spark._
import org.elasticsearch.spark.sql._
import org.apache.spark.sql.SaveMode

/*
 * Avoir une table avec la liste des filtres,
 projets, caisses et date de dernier export de NPDM
 * 
 * Table utile	db_raw_irn_53501_npd.NPDM_LAST_HISTO
 * Table cible	db_raw_irn_53501_npd.PLMD_LIST_INFO
 *Alimentation de la table	OVERWRITE
 *   Delestage de la table	JAMAIS
 *   But du batch	

Récupérer la liste unique des couples Famille/Caisse/Filtre

Overwrite de la colonne Export_Date
User Story	PLMD-17 / PLMD-18
Nom des colonnes utiles	

NPDM_LAST_HISTO.Filter_name

NPDM_LAST_HISTO.Family_name

NPDM_LAST_HISTO.Body_name

NPDM_LAST_HISTO.Timestamp_extraction
Algorithme	
Récupérer la liste des filtres de NPDM
Select distinct filter_name, family_name, body_name, timestamp_extraction from NPDM_LAST_HISTO
Avec la liste de filtres retournées faire un OVERWRITE de la table db_raw_irn_53501_npd.PLMD_LIST_INFO
 * 
 * 
 * 
 * 
 */



object App {

	def main(args : Array[String]) {


		// creation of configuration

		val conf       = new SparkConf()
		

				// creation of spark context and sqlContext

				val sc         = new SparkContext(conf)

				val sqlContext = new SQLContext(sc)

				sqlContext.setConf("spark.sql.caseSensitive", "true");

		// creation of hive context

		val hiveContext  = new org.apache.spark.sql.hive.HiveContext(sc)

				

				// extraction of local source data 
    val plmd_17_18DF = hiveContext.sql("select filter_name,family_name,body_name,timestamp_extraction from  db_raw_irn_53501_npd.npdm_last_histo")
				                                                                                                    
      /*
		    val plmd_17_18DF = hiveContext.read
      .format("com.databricks.spark.csv").option("delimiter", ";")
      .option("header", "true")
      .option("inferSchema", "true")
      .load("/Users/user/Documents/DOC_CONSULTANT/Doc_projet_PLM/doc_datalake/datalake1207.csv")
      */
   

		 import sqlContext.implicits._
    
    val plmd_17_18DFsRdd=plmd_17_18DF
    .select(
     "filter_name","family_name","body_name","timestamp_extraction").rdd
     .map(f=>((
          f(0).toString()),
          f(1).toString(),
          f(2).toString(),
          timeStempFormat(f(3).toString())
         
          ))
          //.toDF("filter_name","family_name","body_name","timestamp_extraction")
          
     val plmd_17_18DFs = hiveContext.createDataFrame(plmd_17_18DFsRdd)
          .withColumnRenamed("_1", "filter_name")
          .withColumnRenamed("_2", "family_name")
          .withColumnRenamed("_3", "body_name")
          .withColumnRenamed("_4", "timestamp_extraction")
          
     
     plmd_17_18DFs.printSchema()
     plmd_17_18DFs.show()     
          

     
      
		import org.apache.spark.sql.functions.unix_timestamp

		val ts = unix_timestamp($"timestamp_extraction", "MM/dd/yyyy HH:mm:ss").cast("timestamp")
		
		
		val plmd_17_18DfParse=plmd_17_18DFs
		                      .withColumn("Timestamp_extraction_parse", ts)
		                       .drop($"timestamp_extraction")
		                       
		                       
		                       
		     plmd_17_18DfParse.printSchema()
         plmd_17_18DfParse.show()                   
                       
		                       
		                       

		import org.apache.spark.sql.functions._
		
		val plmd_17_18DfFilter = plmd_17_18DfParse
		                         .groupBy(plmd_17_18DfParse("filter_name"),plmd_17_18DfParse("family_name"),plmd_17_18DfParse("body_name"))
		                         .agg(max('Timestamp_extraction_parse))


		                         
		                         
		                         

		plmd_17_18DfFilter.show(false)

		val plmd_17_18=plmd_17_18DfFilter
		.withColumnRenamed("Filter_name", "filterName")
		.withColumnRenamed("Family_name", "familyName")
		.withColumnRenamed("Body_name", "bodyName")
		.withColumnRenamed("max(Timestamp_extraction_parse)","timestampExtraction")
		.withColumn("processingTime",current_timestamp)
		
		
		
		
		

    plmd_17_18.printSchema()
		plmd_17_18.show(false)
		
		

		// save data in the datalake in the orc format
		
		import sqlContext.implicits._ 
		plmd_17_18.coalesce(1)
		.write.mode(SaveMode.Overwrite)
		.orc("hdfs://bigopeclu/data/refinery/irn_67133_d4b/work/perimeter/")


		//creation of table  and  insertion of data


		
		
		//path of location hive table in the datalake
		
		
		val path        = "'hdfs://bigopeclu/data/refinery/irn_67133_d4b/work/perimeter/'"
		
		//val skipHeader  = " TBLPROPERTIES ('skip.header.line.count'='1')"

		

    val fieldsAndTypes = "filterName STRING, familyName STRING, bodyName STRING, timestampExtraction TIMESTAMP,processingTime TIMESTAMP"
    val request : String = "CREATE EXTERNAL TABLE IF NOT EXISTS db_work_irn_67133_d4b.plmd_list_info (" + fieldsAndTypes + ")" +
                           """  
                               STORED AS ORC  
                             LOCATION """ + path //+ skipHeader

    hiveContext.sql(request)
    
    
    
    // indexation in ELK
    
    //EsSparkSQL.saveToEs(plmd_17_18, "d4b_int_perimeter/1") 
    
plmd_17_18.write.format("org.elasticsearch.spark.sql")
      .mode(SaveMode.Overwrite)
      //.save("d4b_dev_perimeter_template_test/1") 
      .save("d4b_ope_perimeter/1")



	}



def timeStempFormat(input:String):String={

			val yy=input.substring(4,6)

					val MM=input.substring(2,4)
					val dd=input.substring(0,2)
					val HH=input.substring(7,9)
					val mm=input.substring(9,11)
					val ss=input.substring(11)

					
					
					val timeStp=MM+"/"+dd+"/"+"20"+yy+" "+HH+":"+mm+":"+ss
					
				

					return timeStp
	}


case class NPDM_L_H(Filter_name:String,Family_name:String , Body_name:String ,Timestamp_extraction :String)


}
