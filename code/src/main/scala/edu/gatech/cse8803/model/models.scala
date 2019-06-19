/**
 * @author Hang Su <hangsu@gatech.edu>.
 */

package edu.gatech.cse8803.model

import java.sql.Date



case class Chart(patientID: String, lastChartDate: Date)

case class LabResult(patientID: String, hadmID: String, date: Date, labName: String, value: Double)

case class Diagnostic(patientID: String, hadmID: String, date: Date, icd9code: String, sequence: Int)

case class Medication(patientID: String, hadmID: String, date: Date, medicine: String)


case class Note(patientID: String, hadmID: String, chartDate: Date, category: String, description: String, text: String)
case class TokenizedNote(patientID: String, hadmID: String, chartDate: Date, tokens: Seq[String])


case class PatientId(patientID: String)

case class Patient(patientID: String, isMale: Int, dob: Date, isDead: Int, dod: Date, indexDate: Date, age: Double)

case class IcuStay(patientID: String, hadmID: String, icuStayID: String, inDate: Date, outDate: Date)

case class Saps2(patientID: String, hadmID: String, icuStayID: String, score: Double, scoreProbability: Double, ageScore: Double,
                 hrScore: Double, sysbpScore: Double, tempScore: Double, pao2fio2Score: Double, uoScore: Double,
                 bunScore: Double, wbcScore: Double, potassiumScore: Double, sodiumScore: Double,
                 bicarbonateScore: Double, bilirubinScore: Double, gcsScore: Double, comorbidityScore:
                 Double, admissiontypeScore: Double)

//need to figure out
//class can not have that many params
case class Comorbidities(patientID: String, hadmID: String, allValues: String)
