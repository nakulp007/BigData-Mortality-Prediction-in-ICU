package edu.gatech.cse8803.ioutils

import edu.gatech.cse8803.model._
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.text.SimpleDateFormat

object DataLoader {
  def loadRddRawData(sqlContext: SQLContext, inputPath: String): (RDD[Patient], RDD[Note], RDD[Comorbidities], RDD[IcuStay], RDD[Saps2]) = {

    println("---------- Loading RDD Raw Data ----------")

    val dateFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss")

    /*    PATIENTS    */
    val patients = loadPatientRDD(sqlContext, inputPath, dateFormat)
    //patient.foreach(println)
    println(s"Patient count: ${patients.count}")


    /*     NOTES    */
    val notes = loadNoteRDD(sqlContext, inputPath, dateFormat)
    //notes.foreach(println)
    //println(s"Notes count: ${notes.count}")


    /*     COMORBIDITIES    */
    //need to figure out
    //class can not have that many params
    val comorbidities = loadComorbiditiesRDD(sqlContext, inputPath, dateFormat)
    //comorbidities.foreach(println)
    //println(s"Comorbidities count: ${comorbidities.count}")


    /*     ICU STAYS    */
    val icuStays = loadIcuStayRDD(sqlContext, inputPath, dateFormat)
    //icuStays.foreach(println)
    println(s"ICU stays count: ${icuStays.count}")


    /*     SAPS    */
    val saps2s = loadSaps2RDD(sqlContext, inputPath, dateFormat)
    //saps.foreach(println)
    println(s"SAPS count: ${saps2s.count}")


    println(s"---------- Done Loading RDD Raw Data ----------")

    (patients, notes, comorbidities, icuStays, saps2s)
  }

  def loadPatientRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Patient] = {
    List(inputPath + "/PATIENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "PATIENTS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, GENDER, DOB, DOD, EXPIRE_FLAG
        |FROM PATIENTS
      """.stripMargin)
      .map(line => Patient(
        line(0).toString,
        if (line(1).toString.toLowerCase=="m") 1 else 0,
        new Date(dateFormat.parse(line(2).toString).getTime),
        line(4).toString.toInt,
        if (line(4).toString.toInt == 1 && line(3).toString.trim != "")
          new Date(dateFormat.parse(line(3).toString).getTime)
        else
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime),
        new Date(dateFormat.parse("1971-01-01 00:00:00").getTime),
        0.0
      ))
  }

	//subject_id, hadm_id, chartdate, charttime, category, description, text, storetime
	//SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, CATEGORY, DESCRIPTION, TEXT, STORETIME
  def loadNoteRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Note] = {
    List(inputPath + "/NOTEEVENTS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "NOTEEVENTS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, CHARTDATE, CHARTTIME, CATEGORY, DESCRIPTION, TEXT, STORETIME
        |FROM NOTEEVENTS
      """.stripMargin)
    .map(line => line)
      .filter(line => !line(4).toString.toLowerCase.contains("discharge"))
      .filter(line => !line(1).toString.isEmpty) // outpatients and patients not admitted to ICU don't have HADM_ID
      .map(line => Note(
        line(0).toString,
        line(1).toString,
        if (line(3).toString.isEmpty) {
          if (line(7).toString.isEmpty) { // Check if STORETIME is available to use.
            val dateString = if (line(2).toString.trim.length == 10) line(2).toString.trim + " 00:00:00" else line(2).toString.trim
            new Date(dateFormat.parse(dateString).getTime)
          } else {
            val dateString = if (line(7).toString.trim.length == 10) line(7).toString.trim + " 00:00:00" else line(7).toString.trim
            new Date(dateFormat.parse(dateString).getTime)
          }
        } else {
          val dateString = if (line(3).toString.trim.length == 10) line(3).toString.trim + " 00:00:00" else line(3).toString.trim
          new Date(dateFormat.parse(dateString).getTime)
        },
        line(4).toString,
        line(5).toString,
        line(6).toString
      ))
  }

  def loadComorbiditiesRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Comorbidities] = {
    List(inputPath + "/EHCOMORBIDITIES.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "EHCOMORBIDITIES"))

    sqlContext.sql(
      """
        |SELECT subject_id,hadm_id,congestive_heart_failure,cardiac_arrhythmias,valvular_disease,
        |pulmonary_circulation,peripheral_vascular,hypertension,paralysis,other_neurological,
        |chronic_pulmonary,diabetes_uncomplicated,diabetes_complicated,hypothyroidism,renal_failure,
        |liver_disease,peptic_ulcer,aids,lymphoma,metastatic_cancer,solid_tumor,rheumatoid_arthritis,
        |coagulopathy,obesity,weight_loss,fluid_electrolyte,blood_loss_anemia,deficiency_anemias,
        |alcohol_abuse,drug_abuse,psychoses,depression
        |FROM EHCOMORBIDITIES
      """.stripMargin)
      .map(line => Comorbidities(
        line(0).toString,
        line(1).toString,
        (if (line(2).toString.isEmpty) "0" else line(2).toString) +
        (if (line(3).toString.isEmpty) "0" else line(3).toString) +
        (if (line(4).toString.isEmpty) "0" else line(4).toString) +
        (if (line(5).toString.isEmpty) "0" else line(5).toString) +
        (if (line(6).toString.isEmpty) "0" else line(6).toString)+
        (if (line(7).toString.isEmpty) "0" else line(7).toString)+
        (if (line(8).toString.isEmpty) "0" else line(8).toString)+
        (if (line(9).toString.isEmpty) "0" else line(9).toString)+
        (if (line(10).toString.isEmpty) "0" else line(10).toString) +
        (if (line(11).toString.isEmpty) "0" else line(11).toString)+
        (if (line(12).toString.isEmpty) "0" else line(12).toString)+
        (if (line(13).toString.isEmpty) "0" else line(13).toString)+
        (if (line(14).toString.isEmpty) "0" else line(14).toString)+
        (if (line(15).toString.isEmpty) "0" else line(15).toString)+
        (if (line(16).toString.isEmpty) "0" else line(16).toString)+
        (if (line(17).toString.isEmpty) "0" else line(17).toString)+
        (if (line(18).toString.isEmpty) "0" else line(18).toString)+
        (if (line(19).toString.isEmpty) "0" else line(19).toString)+
        (if (line(20).toString.isEmpty) "0" else line(20).toString)+
        (if (line(21).toString.isEmpty) "0" else line(21).toString)+
        (if (line(22).toString.isEmpty) "0" else line(22).toString)+
        (if (line(23).toString.isEmpty) "0" else line(23).toString)+
        (if (line(24).toString.isEmpty) "0" else line(24).toString)+
        (if (line(25).toString.isEmpty) "0" else line(25).toString)+
        (if (line(26).toString.isEmpty) "0" else line(26).toString)+
        (if (line(27).toString.isEmpty) "0" else line(27).toString) +
        (if (line(28).toString.isEmpty) "0" else line(28).toString)+
        (if (line(29).toString.isEmpty) "0" else line(29).toString)+
        (if (line(30).toString.isEmpty) "0" else line(30).toString)+
        (if (line(31).toString.isEmpty) "0" else line(31).toString)

      ))
  }

  def loadIcuStayRDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[IcuStay] = {
    List(inputPath + "/ICUSTAYS.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "ICUSTAYS"))

    sqlContext.sql(
      """
        |SELECT SUBJECT_ID, HADM_ID, ICUSTAY_ID, INTIME, OUTTIME
        |FROM ICUSTAYS
      """.stripMargin)
      .map(line => IcuStay(
        line(0).toString,
        line(1).toString,
        line(2).toString,
        if (line(3).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(line(3).toString).getTime),
        if (line(4).toString.isEmpty)
          new Date(dateFormat.parse("9000-01-01 00:00:00").getTime)
        else
          new Date(dateFormat.parse(line(4).toString).getTime)
      ))
  }

  def loadSaps2RDD(sqlContext: SQLContext, inputPath: String, dateFormat: SimpleDateFormat): RDD[Saps2] = {
    List(inputPath + "/SAPSII.csv")
      .foreach(CSVUtils.loadCSVAsTable(sqlContext, _, "SAPSII"))

    sqlContext.sql(
      """
        |SELECT subject_id, hadm_id, icustay_id, sapsii, sapsii_prob, age_score,
          hr_score,sysbp_score,temp_score, pao2fio2_score,uo_score,bun_score,
          wbc_score,potassium_score,sodium_score,bicarbonate_score,
          bilirubin_score,gcs_score,comorbidity_score,admissiontype_score
        |FROM SAPSII
      """.stripMargin)
      .map(line => Saps2(
        line(0).toString,
        line(1).toString,
        line(2).toString,
        if (line(3).toString.isEmpty) 0.0 else line(3).toString.toDouble,
        if (line(4).toString.isEmpty) 0.0 else line(4).toString.toDouble,
        if (line(5).toString.isEmpty) 0.0 else line(5).toString.toDouble,
        if (line(6).toString.isEmpty) 0.0 else line(6).toString.toDouble,
        if (line(7).toString.isEmpty) 0.0 else line(7).toString.toDouble,
        if (line(8).toString.isEmpty) 0.0 else line(8).toString.toDouble,
        if (line(9).toString.isEmpty) 0.0 else line(9).toString.toDouble,
        if (line(10).toString.isEmpty) 0.0 else line(10).toString.toDouble,
        if (line(11).toString.isEmpty) 0.0 else line(11).toString.toDouble,
        if (line(12).toString.isEmpty) 0.0 else line(12).toString.toDouble,
        if (line(13).toString.isEmpty) 0.0 else line(13).toString.toDouble,
        if (line(14).toString.isEmpty) 0.0 else line(14).toString.toDouble,
        if (line(15).toString.isEmpty) 0.0 else line(15).toString.toDouble,
        if (line(16).toString.isEmpty) 0.0 else line(16).toString.toDouble,
        if (line(17).toString.isEmpty) 0.0 else line(17).toString.toDouble,
        if (line(18).toString.isEmpty) 0.0 else line(18).toString.toDouble,
        if (line(19).toString.isEmpty) 0.0 else line(19).toString.toDouble
      ))
  }

}
