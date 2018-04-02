from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.python.utils import dataset
from pyspark.ml.classification import RandomForestClassifier
from pyspark.ml.evaluation import BinaryClassificationEvaluator
from pyspark.ml.tuning import CrossValidator, ParamGridBuilder
import datetime

FEATURES_SPARK_TRAIN_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/spark_ft_train.csv"
FEATURES_SPARK_TEST_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/spark_ft_test.csv"


def savemodel(model,name):
    model.save("models/{}_{}".format(name,datetime.datetime.now().strftime("%d_%B_%Y")))


def evaluate(model,test):
    predictions = model.transform(test)
    evaluator = BinaryClassificationEvaluator(metricName="accuracy")
    accuracy = evaluator.evaluate(predictions)
    print("Test Error = %g" % (1.0 - accuracy))


def gridsearch(model, train):
    paramGrid = (ParamGridBuilder()
        .addGrid(model.numTrees, [x for x in range(3,50)])
        .addGrid(model.impurity, ["gini","entropy"])
        .addGrid(model.maxDepth, [x for x in range(4,25)])
        .addGrid(model.maxBins, [x for x in range(32,128,8)])
        .build()
        )

    crossval = CrossValidator(estimator=model,
                              estimatorParamMaps=paramGrid,
                              evaluator=BinaryClassificationEvaluator(),
                              numFolds=5)  # use 3+ folds in practice

    # Run cross-validation, and choose the best set of parameters.
    cvModel = crossval.fit(train)
    return cvModel


def model(spark, use_gridsearch = False):
    train = dataset.load(spark,FEATURES_SPARK_TRAIN_PATH,"parquet")
    test = dataset.load(spark, FEATURES_SPARK_TEST_PATH, "parquet")
    if use_gridsearch:
        model = gridsearch(RandomForestClassifier,train)
    else:
        model = RandomForestClassifier().fit(train)

    evaluate(model,test)
    savemodel(model,"Randomforest")


if __name__ == '__main__':

    # Init the spark session
    spark = SparkSession.builder \
        .appName("SparkSession") \
        .enableHiveSupport() \
        .getOrCreate()

    model(spark,False)

