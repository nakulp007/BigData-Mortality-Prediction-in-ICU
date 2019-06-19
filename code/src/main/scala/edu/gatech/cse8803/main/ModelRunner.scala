package edu.gatech.cse8803.main

import edu.gatech.cse8803.ioutils.{CSVUtils, DataLoader}
import edu.gatech.cse8803.features.FeatureConstruction._

import edu.gatech.cse8803.model._
import org.apache.spark.ml.param.{Param}
import org.apache.spark.ml.tuning.{CrossValidator}
import org.apache.spark.mllib.evaluation.BinaryClassificationMetrics
import org.apache.spark.mllib.classification.{SVMWithSGD, SVMModel}
import org.apache.spark.mllib.linalg.{Vector, Vectors}
import org.apache.spark.mllib.regression.LabeledPoint
import org.apache.spark.rdd.RDD
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}

import java.sql.Date
import java.text.SimpleDateFormat

object ModelRunner {
  def runBaseLineModel(sc: SparkContext, trainPatients: RDD[Patient], testPatients: RDD[Patient],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2], firstNoteDates: RDD[FirstNoteInfo],
      labels: RDD[LabelTuple],
      useCv: Boolean=false, numFolds: Int=5,
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=100.0) = {
    println("B---------------- Begin baseline model ------------------------")
    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    /************************* Create model ************************/
    // Split into folds
    if (useCv) {
      val splitProportions = new Array[Double](numFolds)
      for (i <- 0 to (numFolds - 1)) splitProportions(i) = 1.0 / numFolds
      val splits = trainPatients.randomSplit(splitProportions)

      val arrayCvTrainPoints = new Array[RDD[LabeledPoint]](numFolds)
      val arrayCvTestPoints = new Array[RDD[LabeledPoint]](numFolds)
      for (i <- 0 to (numFolds - 1)) {
        val cvTrainTuples = constructBaselineFeatureTuples(
          sc, trainPatients.subtract(splits(i)), icuStays, saps2s)
        arrayCvTrainPoints(i) = constructForSVMSparse(cvTrainTuples, labels)

        val cvTestTuples = constructBaselineFeatureTuples(sc, splits(i), icuStays, saps2s)
        arrayCvTestPoints(i) = constructForSVMSparse(cvTestTuples, labels)
        //arrayCvTestPoints(i).cache
      }

      println("B------- Run cross validation for training model --------")
      /************ Cross validate for the best hyper parameters ***********/
      val interceptConds: List[Boolean] = List(true) // false is default
      val numIterationsParams: List[Int] = List(200, 300, 400) // 100 is default
      val regParams: List[Double] = List(0.1, 1, 10, 50, 100) // 0.0 is default

      var maxAUC = 0.0
      bestInterceptCond = true
      bestNumIterations = 200
      bestRegParam = 0.1

      var sumAUC = 0.0
      for (ic <- interceptConds) {
        for (ni <- numIterationsParams) {
          for (rp <- regParams) {
            val svm = new SVMWithSGD()
            svm.optimizer
              .setNumIterations(ni)
              .setRegParam(rp)
            svm.setIntercept(ic)

            sumAUC = 0.0
            for (i <- 0 to (numFolds - 1)) {
              arrayCvTrainPoints(i).cache
              val svmModel = svm.run(arrayCvTrainPoints(i))
              svmModel.clearThreshold

              val preds = arrayCvTestPoints(i)
                .map(point => (svmModel.predict(point.features), point.label))
              val metrics = new BinaryClassificationMetrics(preds)
              sumAUC += metrics.areaUnderROC
            }
            val auc = sumAUC / numFolds // get average AUC
            println(s"${ic},${ni},${rp},${auc}")
            if (auc > maxAUC) {
              maxAUC = auc
              bestInterceptCond = ic
              bestNumIterations = ni
              bestRegParam = rp
            }
          }
        }
      }
    }

    println(s"Chosen interceptCond: ${bestInterceptCond}")
    println(s"Chosen numIterations: ${bestNumIterations}")
    println(s"Chosen regParam: ${bestRegParam}")

    // Use the best parameters to construct a model for the whole training set
    val trainTuples = constructBaselineFeatureTuples(sc, trainPatients, icuStays, saps2s)
    val trainingPoints = constructForSVMSparse(trainTuples, labels)
    trainingPoints.cache

    val svm = new SVMWithSGD()
    svm.optimizer
      .setNumIterations(bestNumIterations)
      .setRegParam(bestRegParam)
    svm.setIntercept(bestInterceptCond)

    val svmModel = svm.run(trainingPoints)
    svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

    println("B----------- Test with time varying testing set --------------")
    // Evaluate the trained model with the training set
    val trainPreds = trainingPoints
      .map(point => (svmModel.predict(point.features), point.label))
    var (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(trainPreds)
    println(s"Number of training instances: ${total}")
    println(s"Number of positive in train: ${numPositives}")
    println(s"Final training AUC: ${auc}")
    println("${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    println(s"${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")

    println("${hr},${total},${numPositives},${auc},${accuracy},${sensitivity},${specificity}")
    // Test the model across time with the test data
    var go = true
    var hr = 0
    val maxH = 240
    while (hr <= maxH && go) {
      val testTuples = constructBaselineFeatureTuples(sc, testPatients, icuStays,
        saps2s, firstNoteDates, hr)
      val testingPoints = constructForSVMSparse(testTuples, labels)
      // Make predictions using the SVM Model
      val testPreds = testingPoints
        .map(point => (svmModel.predict(point.features), point.label))

      var (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(testPreds)
      println(s"${hr},${total},${numPositives},${auc}")
      hr += 12
    }
    println("B---------------- Baseline model test completed -------------------")
    //println(s"${hours},${numTraining},${numTrainingPositive},${numTesting},${numTestingPositive},${trainAUC},${testAUC}")
  }

  def getBinaryMetrics(predLabels: RDD[(Double, Double)]): (Int, Int, Double, Double, Double, Double) = {
    // (tp, fp, tn, TF)
    val (tp, fp, tn, fn) = predLabels.aggregate((0, 0, 0, 0))(
      (u, pair) => {
        val correct = pair._1 == pair._2
        val positive = pair._2 > 0
        if (correct) {
          if (positive) {
            (u._1+1, u._2, u._3, u._4)
          } else {
            (u._1, u._2, u._3+1, u._4)
          }
        } else {
          if (positive) {
            (u._1, u._2, u._3, u._4+1)
          } else {
            (u._1, u._2+1, u._3, u._4)
          }
        }
      },
      (u1, u2) => (u1._1+u2._1, u1._2+u2._2, u1._3+u2._3, u1._4+u2._4)
    )

    val binMetrics = new BinaryClassificationMetrics(predLabels)
    val auc = binMetrics.areaUnderROC

    val total = tp + fp + tn + fn
    val numPositives = tp + fn
    val numNegatives = fp + tn

    val accuracy = (tp + tn).toDouble / total.toDouble
    val sensitivity = tp.toDouble / numPositives // recall
    val specificity = tn.toDouble / numNegatives
    //val precision = tp / (tp + fp)
    (total, numPositives, auc, accuracy, sensitivity, specificity)
  }

  def runTimeVaryingTopicModel(sc: SparkContext, trainPatients: RDD[Patient], testPatients: RDD[Patient],
      labels: RDD[LabelTuple],
      icuStays: RDD[IcuStay], firstNoteDates: RDD[FirstNoteInfo],
      tokenizedNotes: RDD[TokenizedNote],
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=100.0) = {
    println("B---------------- Begin time varying topic model ------------------------")
    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    /************************* Create model ************************/
    println(s"Chosen interceptCond: ${bestInterceptCond}")
    println(s"Chosen numIterations: ${bestNumIterations}")
    println(s"Chosen regParam: ${bestRegParam}")

    println("${hr},${total},${numPositives},${auc}")
    val filteredTrainNotes = tokenizedNotes.keyBy(_.patientID)
      .join(trainPatients.keyBy(_.patientID))
      .map{ case(pid, (tnote, p)) => tnote }
    val filteredTestNotes = tokenizedNotes.keyBy(_.patientID)
      .join(testPatients.keyBy(_.patientID))
      .map{ case(pid, (tnote, p)) => tnote }

    // Test the model across time with the test data
    var go = true
    var hr = 12
    val maxH = 240
    while (hr <= maxH && go) {
      val currentTrainNotes = filterTokenizedNotesOnHoursSinceFirstNote(
          filteredTrainNotes, firstNoteDates, hr)
      val trainTuples = retrospectiveTopicModel(currentTrainNotes)
      val trainingPoints = constructForSVM(trainTuples, labels)
      trainingPoints.cache

      val svm = new SVMWithSGD()
      svm.optimizer
        .setNumIterations(bestNumIterations)
        .setRegParam(bestRegParam)
      svm.setIntercept(bestInterceptCond)

      val svmModel = svm.run(trainingPoints)
      svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

      println("B----------- Test with time varying testing set --------------")
      // Evaluate the trained model with the training set
      val trainPreds = trainingPoints
        .map(point => (svmModel.predict(point.features), point.label))
      var (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(trainPreds)

      val currentTestNotes = filterTokenizedNotesOnHoursSinceFirstNote(
          filteredTestNotes, firstNoteDates, hr)
      val testTuples = retrospectiveTopicModel(currentTestNotes)
      val testingPoints = constructForSVM(testTuples, labels)
      // Make predictions using the SVM Model
      val testPreds = testingPoints
        .map(point => (svmModel.predict(point.features), point.label))

      var (testTotal, testNumPositives, testAuc, testAccuracy, testSensitivity, testSpecificity) = getBinaryMetrics(testPreds)
      println(s"${hr},${total},${numPositives},${testTotal},${testNumPositives},${auc},${testAuc}")
      hr += 12
    }
    println("B---------------- Time varying topic model test completed -------------------")
    //println(s"${hours},${numTraining},${numTrainingPositive},${numTesting},${numTestingPositive},${trainAUC},${testAUC}")
  }

  def runRetrospectiveTopicModel(sc: SparkContext,
      trainPatients: RDD[Patient], testPatients: RDD[Patient],
      tokenizedNotes: RDD[TokenizedNote], labels: RDD[LabelTuple],
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=100.0) = {
    println("RTM--------------- Begin retrospective topic model ----------------")

    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    println(s"Chosen interceptCond: ${bestInterceptCond}")
    println(s"Chosen numIterations: ${bestNumIterations}")
    println(s"Chosen regParam: ${bestRegParam}")

    val allLdaFeatures = retrospectiveTopicModel(tokenizedNotes)

    val trainNoteFeatures = allLdaFeatures.join(trainPatients.keyBy(_.patientID))
      .map{ case(pid, (arr, p)) => (pid, arr) }
    println(s"Train note features count: ${trainNoteFeatures.count}")

    val trainingPoints = constructForSVM(trainNoteFeatures, labels)
    trainingPoints.cache

    val svm = new SVMWithSGD()
    svm.optimizer
      .setNumIterations(bestNumIterations)
      .setRegParam(bestRegParam)
    svm.setIntercept(bestInterceptCond)

    val svmModel = svm.run(trainingPoints)
    svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

    // Evaluate the trained model with the training set
    val trainPreds = trainingPoints
      .map(point => (svmModel.predict(point.features), point.label))
    val (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(trainPreds)

    val testNoteFeatures = allLdaFeatures.join(testPatients.keyBy(_.patientID))
      .map{ case(pid, (arr, p)) => (pid, arr) }
    println(s"Test note features count: ${testNoteFeatures.count}")
    val testingPoints = constructForSVM(testNoteFeatures, labels)
    val testPreds = testingPoints
      .map(point => (svmModel.predict(point.features), point.label))

    var (testTotal, testNumPositives, testAuc, testAccuracy, testSensitivity, testSpecificity) = getBinaryMetrics(testPreds)
    println(s"Retrospective topic model test AUC: ${testAuc}")
    println("${total},${numPositives},${testTotal},${testNumPositives},${auc},${testAuc}")
    println(s"${total},${numPositives},${testTotal},${testNumPositives},${auc},${testAuc}")
    println("RTM---------------- Retrospective topic model completed -------------------")
  }

  def runRetrospectiveDerivedFeatureModel(sc: SparkContext,
      trainPatients: RDD[Patient], testPatients: RDD[Patient], labels: RDD[LabelTuple],
      icuStays: RDD[IcuStay], saps2s: RDD[Saps2], comorbidities: RDD[Comorbidities],
      useCv: Boolean=false, numFolds: Int=5,
      interceptCond: Boolean=true, numIterations: Int=300, regParam: Double=0.1) = {
    println("--------------- Begin retrospective derived feature model ----------------")
    val baselineFeatureArrayTuples = constructBaselineFeatureArrayTuples(
      sc, trainPatients, icuStays, saps2s)
    val comorbFeatureArrayTuples = constructDerivedFeatures(trainPatients, icuStays, comorbidities)
    val combinedFeatureArrayTuples = baselineFeatureArrayTuples
      .join(comorbFeatureArrayTuples)
      .map{ case(pid, (carr, barr)) => (pid, carr ++ barr) }

    println(s"combinedFeatureArrayTuples count: ${combinedFeatureArrayTuples.count}")
    //combinedFeatureArrayTuples.take(10).foreach(x => println(s"${x._2.size}--${x._2.mkString(" ")}"))

    var bestInterceptCond = interceptCond
    var bestNumIterations = numIterations
    var bestRegParam = regParam

    if (useCv) {
      /************************* Create model ************************/
      // Split into folds
      val splitProportions = new Array[Double](numFolds)
      for (i <- 0 to (numFolds - 1)) splitProportions(i) = 1.0 / numFolds
      val splits = trainPatients.randomSplit(splitProportions)

      val arrayCvTrainPoints = new Array[RDD[LabeledPoint]](numFolds)
      val arrayCvTestPoints = new Array[RDD[LabeledPoint]](numFolds)
      for (i <- 0 to (numFolds - 1)) {
        val cvTrainTuples = trainPatients.subtract(splits(i))
          .keyBy(_.patientID)
          .join(combinedFeatureArrayTuples)
          .map{ case(pid, (p, arr)) => (pid, arr) }
        arrayCvTrainPoints(i) = constructForSVM(cvTrainTuples, labels)

        val cvTestTuples = splits(i)
          .keyBy(_.patientID)
          .join(combinedFeatureArrayTuples)
          .map{ case(pid, (p, arr)) => (pid, arr) }
        arrayCvTestPoints(i) = constructForSVM(cvTestTuples, labels)
      }

      println("------- Run cross validation for training model --------")
      /************ Cross validate for the best hyper parameters ***********/
      val interceptConds: List[Boolean] = List(false, true) // false is default
      val numIterationsParams: List[Int] = List(200, 300, 400) // 100 is default
      val regParams: List[Double] = List(0.1, 1, 10, 100) // 0.0 is default

      var maxAUC = 0.0
      bestInterceptCond = false
      bestNumIterations = 200
      bestRegParam = 0.1

      var sumAUC = 0.0
      for (interceptCond <- interceptConds) {
        for (numIterations <- numIterationsParams) {
          for (regParam <- regParams) {
            val svm = new SVMWithSGD()
            svm.optimizer
              .setNumIterations(numIterations)
              .setRegParam(regParam)
            svm.setIntercept(interceptCond)

            sumAUC = 0.0
            for (i <- 0 to (numFolds - 1)) {
              arrayCvTrainPoints(i).cache
              val svmModel = svm.run(arrayCvTrainPoints(i))
              svmModel.clearThreshold

              val preds = arrayCvTestPoints(i)
                .map(point => (svmModel.predict(point.features), point.label))
              val metrics = new BinaryClassificationMetrics(preds)
              sumAUC += metrics.areaUnderROC
            }
            val auc = sumAUC / numFolds // get average AUC
            println(s"${interceptCond},${numIterations},${regParam},${auc}")
            if (auc > maxAUC) {
              maxAUC = auc
              bestInterceptCond = interceptCond
              bestNumIterations = numIterations
              bestRegParam = regParam
            }
          }
        }
      }
    }
    println(s"Best interceptCond: ${bestInterceptCond}")
    println(s"Best numIterations: ${bestNumIterations}")
    println(s"Best regParam: ${bestRegParam}")

    // Use the best parameters to construct a model for the whole training set
    val trainingPoints = constructForSVM(combinedFeatureArrayTuples, labels)
    trainingPoints.cache

    // Count number of training instances and positive ones among them
    val (numTraining, numTrainingPositive) = trainingPoints
      .aggregate((0, 0))(
        (u, point) => (u._1+1, u._2+point.label.toInt),
        (u1, u2) => (u1._1+u2._1, u1._2+u2._2)
      )
    println(s"Number of training instances: ${numTraining}")
    println(s"Number of positive in train: ${numTrainingPositive}")

    val svm = new SVMWithSGD()
    svm.optimizer
      .setNumIterations(bestNumIterations)
      .setRegParam(bestRegParam)
    svm.setIntercept(bestInterceptCond)

    val svmModel = svm.run(trainingPoints)
    svmModel.clearThreshold // Clears threshold so predict() outputs raw prediction scores.

    // Evaluate the trained model with the training set
    val trainPreds = trainingPoints
      .map(point => (svmModel.predict(point.features), point.label))
    val (total, numPositives, auc, accuracy, sensitivity, specificity) = getBinaryMetrics(trainPreds)


    val testBaselineFeatureArrayTuples = constructBaselineFeatureArrayTuples(
      sc, testPatients, icuStays, saps2s)
    val testComorbFeatureArrayTuples = constructDerivedFeatures(testPatients, icuStays, comorbidities)
    val testCombinedFeatureArrayTuples = testBaselineFeatureArrayTuples
      .join(testComorbFeatureArrayTuples)
      .map{ case(pid, (carr, barr)) => (pid, carr ++ barr) }

    val testingPoints = constructForSVM(testCombinedFeatureArrayTuples, labels)
    val testPreds = testingPoints
      .map(point => (svmModel.predict(point.features), point.label))

    var (testTotal, testNumPositives, testAuc, testAccuracy, testSensitivity, testSpecificity) = getBinaryMetrics(testPreds)
    println(s"Retrospective derived features test AUC: ${testAuc}")
    println("${total},${numPositives},${testTotal},${testNumPositives},${auc},${testAuc}")
    println(s"${total},${numPositives},${testTotal},${testNumPositives},${auc},${testAuc}")
    println("---------------- Retrospective derived features model completed -------------------")
  }
}
