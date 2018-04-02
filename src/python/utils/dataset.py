def load(spark,path,type="csv"):
    #Load the dataset from the csv file
    assert(type in ["csv","parquet"], "Type :{} is not of types :{}".format(type,["csv","parquet"]))
    if type == "csv":
        return spark.read.csv(path=path,header=True)
    elif type == "parquet":
        return spark.read.parquet(path=path)

    print("Load from :{}".format(path))


def write(df, path,type="csv"):
    assert ((type in ["csv", "parquet"]), "Type :{} is not of types :{}".format(type, ["csv", "parquet"]))

    if type == "csv":
        df.write.csv(path=path,sep=";",header=True,mode="overwrite")
    elif type == "parquet":
        df.write.parquet(path=path,mode="overwrite")
    print("Written to :{}".format(path))

