// Read from the merged file
var data = spark.read.option("header", "true").option("inferSchema", "true").csv("hdfs://nyu-dataproc-m/user/zs2134_nyu_edu/project/merged/*.csv")
data = data.drop("date")


val featureCols = Array(
  "t_daily_max", "t_daily_min", "t_daily_avg", "p_daily_calc",
  "solarad_daily", "sur_temp_daily_max", "sur_temp_daily_min", "sur_temp_daily_avg",
  "rh_daily_max", "rh_daily_min", "rh_daily_avg", "soil_moisture_50_daily", "soil_temp_50_daily"
)

import org.apache.spark.ml.feature.VectorAssembler

// Assemble predictors into a vector
val assembler = new VectorAssembler().setInputCols(featureCols).setOutputCol("features")
val assembledData = assembler.transform(data)

// Split the data into training and testing sets
val Array(trainingData, testingData) = assembledData.randomSplit(Array(0.8, 0.2), seed = 123)

import org.apache.spark.ml.regression.LinearRegression

// Create a Linear Regression model
val lr = new LinearRegression()
  .setLabelCol("daily_mean_concentration")
  .setFeaturesCol("features")

// Fit the model to the training data
val model = lr.fit(trainingData)

// Making predictions on the test data
val predictions = model.transform(testingData)

// Evaluating the model with RMSE
import org.apache.spark.ml.evaluation.RegressionEvaluator
val evaluator = new RegressionEvaluator()
  .setLabelCol("daily_mean_concentration")
  .setPredictionCol("prediction")
  .setMetricName("rmse")
val rmse = evaluator.evaluate(predictions)
println(s"Root Mean Squared Error (RMSE) on test data: $rmse") // 4.985

// Displaying the coefficients and intercept
println(s"Coefficients: ${model.coefficients}")
println(s"Intercept: ${model.intercept}")

// Displaying the most predictive predictors
println("Most predictive predictors:")
val featureCoefficients = featureCols.zip(model.coefficients.toArray)
  .sortBy(-_._2.abs)
  .foreach { case (feature, coefficient) =>
    println(s"$feature: $coefficient")
  }