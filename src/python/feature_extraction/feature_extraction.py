from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from src.python.utils import dataset
from pyspark.ml.feature import VectorAssembler,StandardScaler
from pyspark.ml import Pipeline
from pyspark.sql.types import IntegerType

TRAIN_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/train.csv"
TEST_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/test.csv"
FEATURES_TRAIN_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/ft_train.csv"
FEATURES_SPARK_TRAIN_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/spark_ft_train.csv"
FEATURES_TEST_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/ft_test.csv"
FEATURES_SPARK_TEST_PATH = "/home/siebert/projects/adtracking-fraud-detection/data/spark_ft_test.csv"
label_ = "is_attributed"

def time_features(df, date_column):
    return (df.withColumn("HourOfDay",F.hour(date_column))
              .withColumn("DayOfWeek",F.dayofmonth(date_column))
              .withColumn("DayOfYear",F.dayofyear(date_column))
              .drop(date_column)
            )

def cast_to_integer(df,columns):
    for column in columns:
        df = df.withColumn(column, F.col(column).cast(IntegerType()))
    return df


def convert_to_spark(df,label_):
    columns = list(set(df.columns) - set([label_]))

    #First cast all the columns to integer
    df = df.fillna(0)
    df = cast_to_integer(df,columns)
    #Assembler the dataset
    assembler = VectorAssembler(inputCols=columns,outputCol="features")
    scaler = StandardScaler(inputCol="features", outputCol="scaledFeatures",
                            withStd=True, withMean=False)
    pipeline = Pipeline(stages=[assembler,scaler])
    # Compute summary statistics by fitting the StandardScaler
    model = pipeline.fit(df)
    return model.transform(df)

def feature_extraction(spark):
    for arr in [[TRAIN_PATH,FEATURES_TRAIN_PATH,FEATURES_SPARK_TRAIN_PATH],[TEST_PATH,FEATURES_TEST_PATH,FEATURES_SPARK_TEST_PATH]]:
        df = dataset.load(spark,arr[0],type="csv")
        df = time_features(df,"click_time")
        dataset.write(df,arr[1],type="parquet")
        df = convert_to_spark(df,"is_attributed")
        dataset.write(df,arr[2],type="parquet")


if __name__ == '__main__':

    # Init the spark session
    spark = SparkSession.builder \
        .appName("SparkSession") \
        .enableHiveSupport() \
        .getOrCreate()

    feature_extraction(spark)

