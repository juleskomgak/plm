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

		val conf       = new SparkConf().setAppName("User Story	PLMD-17 / PLMD-18").setMaster("local[*]")
				.set("es.index.auto.create","true")
				.set("es.nodes", "s1908ars.mc2.renault.fr")
				.set("es.port","9200")
				.set("es.cluster.name","CLU1_RE7_ELK")
				.set("es.net.ssl","true")
				.set("es.net.http.auth.user","awdab01")
				.set("es.net.http.auth.pass","hwcz51xo")


				// creation of spark context and sqlContext

				val sc         = new SparkContext(conf)

				val sqlContext = new SQLContext(sc)

				sqlContext.setConf("spark.sql.caseSensitive", "true");

		// creation of hive context

		val hiveContext  = new org.apache.spark.sql.hive.HiveContext(sc)

				import  sqlContext.implicits._

				// extraction of local source data 

				val plmd_17_18RDD=sc.textFile("file:///Users/user/Documents/DOC_CONSULTANT/Doc_projet_PLM/JDD_OCIIND_PROJET.csv")

				//val plmd_17_18RDD=sc.textFile("hdfs:///tmp/plmdashboarding/JDD_OCIIND_PROJET.csv")

			// suppression of header and creation of scala rdd for our data
				
				val plmd_17_18RDDmap=plmd_17_18RDD.map(line=>line.split(";"))
				.mapPartitionsWithIndex { (idx, iter) => if (idx == 0) iter.drop(1) else iter }
		    .map(line=>NPDM_L_H(line(217).distinct,line(218),line(219),(timeStempFormat(line(221)))))
		    
		    
		// creation of dataFrame with hive context 

		val plmd_17_18DF = hiveContext.createDataFrame(plmd_17_18RDDmap)

		// parsing of the Timestamp_extraction column in  TimeStamp format

		import org.apache.spark.sql.functions.unix_timestamp

		val ts = unix_timestamp($"Timestamp_extraction", "MM/dd/yyyy HH:mm:ss").cast("timestamp")

		// creation of the new  column parser and suppression of last column

		val plmd_17_18DfParse=plmd_17_18DF
		                      .withColumn("Timestamp_extraction_parse", ts)
		                       .drop($"Timestamp_extraction")



		import org.apache.spark.sql.functions._
		
		val plmd_17_18DfFilter = plmd_17_18DfParse
		                         .groupBy(plmd_17_18DfParse("Filter_name"),plmd_17_18DfParse("Family_name"),plmd_17_18DfParse("Body_name"))
		                         .agg(max('Timestamp_extraction_parse))



		//plmd_17_18DfFilter.show()

		val plmd_17_18=plmd_17_18DfFilter
		.withColumnRenamed("Filter_name", "filterName")
		.withColumnRenamed("Family_name", "familyName")
		.withColumnRenamed("Body_name", "bodyName")
		.withColumnRenamed("max(Timestamp_extraction_parse)", "updatedAt")
		
		
		// indexation in ELK
		
		plmd_17_18.saveToEs("d4b_test/test1718")


		//plmd_17_18.show()

		// save data in the datalake in the orc format
		
		/*plmd_17_18.repartition(1)
		.write.mode(SaveMode.Overwrite)
		.orc("hdfs:///tmp/hive/testHive/Jdb_raw_irn_53501_npd.PLMD_LIST_INFO")*/

		//plmd_17_18.coalesce(1).write.json("file:///Users/user/Documents/DOC_CONSULTANT/Doc_projet_PLM/plmd1718b")


		//creation of table  and  insertion of data


		//val test_path   = "'/tmp/plmdashboarding/'"
		
		//path of location hive table in the datalake
		val path        = "'/tmp/hive/testHive/'"
		//val skipHeader  = " TBLPROPERTIES ('skip.header.line.count'='1')"

		/* 

    val fieldsAndTypes = "filterName STRING, familyName STRING, bodyName STRING, updatedAt TIMESTAMP"
    val request : String = "CREATE EXTERNAL TABLE IF NOT EXISTS PLMD_LIST_INFO (" + fieldsAndTypes + ")" +
                           """ ROW FORMAT DELIMITED 
                             FIELDS TERMINATED BY ';'
                             STORED AS ORC 
                             LOCATION """ + path //+ skipHeader

    hiveContext.sql(request)

    // test Queries  expressed in HiveQL
    hiveContext.sql("SELECT * FROM PLMD_LIST_INFO LIMIT 3").collect().foreach(println)
    
    LOAD DATA INPATH '/no_prod_data/landing/transit/irn_53501_npd/work/input/rpn' INTO TABLE tab_oci_ind;

		 */





	}




	def timeStempFormat(input:String):String={

			val yyyy=input.substring(4,8)

					val MM=input.substring(2,4)
					val dd=input.substring(0,2)
					val HH=input.substring(9,11)
					val mm=input.substring(11,13)
					val ss=input.substring(13)

					val timeStp=MM+"/"+dd+"/"+yyyy+" "+HH+":"+mm+":"+ss

					return timeStp
	}




case class NPDM_L_H(Filter_name:String,Family_name:String , Body_name:String ,Timestamp_extraction :String)
}
