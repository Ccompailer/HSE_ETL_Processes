from pyspark.sql import SparkSession, Row
from pyspark.sql.functions import col, when, lit
from pyspark.sql.types import StructType, StructField, IntegerType, DateType, StringType, BooleanType


def main():
    spark = SparkSession.builder.appName("dataproc-kafka-write-app").getOrCreate()

    schema = StructType([
        StructField("id", IntegerType(), True),
        StructField("answer_date", DateType(), True),
        StructField("gender", StringType(), True),
        StructField("country", StringType(), True),
        StructField("occupation", StringType(), True),
        StructField("self_employeed", BooleanType(), True),
        StructField("family_history", BooleanType(), True),
        StructField("treatment", BooleanType(), True),
        StructField("days_indoors", StringType(), True),
        StructField("growing_stress", StringType(), True),
        StructField("change_habits", StringType(), True),
        StructField("mental_health_history", StringType(), True),
        StructField("mood_swings", StringType(), True),
        StructField("coping_struggles", BooleanType(), True),
        StructField("work_interest", StringType(), True),
        StructField("social_weaknes", StringType(), True),
        StructField("mental_health_interview", StringType(), True),
        StructField("care_options", StringType(), True)])

    df = spark.read.csv('s3a://etl-baket/2026/06/15/mental_health/part-1781543865-c21f969b.00000.csv', schema=schema)

    df = df.filter((col('gender') == 'Female') & (col('country') == 'United States'))
    df = df.withColumn('predictive_score', when(col('family_history'), lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('treatment'), lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('growing_stress') == 'Yes', lit(10)).when(
        col('growing_stress') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('change_habits') == 'Yes', lit(10)).when(
        col('change_habits') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score',
                       col('predictive_score') + when(col('mental_health_history') == 'Yes', lit(10)).when(
                           col('mental_health_history') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('mood_swings') == 'High', lit(10)).when(
        col('mood_swings') == 'Medium', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score',
                       col('predictive_score') + when(col('coping_struggles'), lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('work_interest') == 'Yes', lit(10)).when(
        col('work_interest') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('social_weaknes') == 'Yes', lit(10)).when(
        col('social_weaknes') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score',
                       col('predictive_score') + when(col('mental_health_interview') == 'Yes', lit(10)).when(
                           col('mental_health_interview') == 'Maybe', lit(5)).otherwise(lit(0)))
    df = df.withColumn('predictive_score', col('predictive_score') + when(col('care_options') == 'Yes', lit(10)).when(
        col('care_options') == 'Not sure', lit(5)).otherwise(lit(0)))

    df_selected = df.select('id', 'answer_date', 'gender', 'country', 'occupation', 'self_employeed',
                            'predictive_score')
    df_selected.write.csv('s3a://etl-baket/mh_predictive.csv')


if __name__ == "__main__":
    main()
