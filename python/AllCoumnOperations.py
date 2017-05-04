# Computes the mean of all columns in a Spark DataFrame
def allMeans(df):
    from pyspark.sql.functions import col, mean
    exprs = [mean(c) for c in df.columns]
    return (df.agg(*exprs).toPandas()).transpose()

# Computes the standard deviation of all columns in a Spark DataFrame
def allSD(df):
    from pyspark.sql.functions import col, mean
    exprs = [sd(c) for c in df.columns]
    return (df.agg(*exprs).toPandas()).transpose()
