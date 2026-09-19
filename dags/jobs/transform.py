import argparse
import sys
from pyspark.sql import SparkSession
from pyspark.sql.functions import split, explode, trim, col

def main():
    parser = argparse.ArgumentParser(description="Netflix dataset transformation with PySpark")
    parser.add_argument("--input_path", type=str, required=True, help="Input CSV file path")
    parser.add_argument("--output_path", type=str, required=True, help="Output Parquet directory path")
    parser.add_argument("--release_year", type=int, default=2015, help="Filter threshold for release_year (default: 2015)")
    args = parser.parse_args()

    spark = SparkSession.builder \
        .appName("WeeklyPipeline-NetflixTransform") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # CSV 데이터 로드
    df = spark.read.option("header", "true") \
                   .option("multiLine", "true") \
                   .option("escape", "\"") \
                   .csv(args.input_path)

    # 1. release_year 필터링
    filtered_df = df.filter(col("release_year").cast("int") >= args.release_year)

    # 2. listed_in 컬럼 쉼표 분리 후 explode 및 공백 제거
    exploded_df = filtered_df.withColumn("genre", explode(split(col("listed_in"), ","))) \
                             .withColumn("genre", trim(col("genre")))

    # 3. type x genre 별 작품 수 집계
    aggregated_df = exploded_df.groupBy("type", "genre") \
                               .count() \
                               .withColumnRenamed("count", "title_count")

    # 결과 확인 및 집계 행 수 출력
    print("=== Aggregated Result Sample ===")
    aggregated_df.show(50, truncate=False)
    
    total_rows = aggregated_df.count()
    print(f"=== Total Aggregated Rows: {total_rows} ===")

    # 4. Parquet (snappy 압축)으로 로컬 경로에 저장
    aggregated_df.write \
        .mode("overwrite") \
        .option("compression", "snappy") \
        .parquet(args.output_path)

    spark.stop()

if __name__ == "__main__":
    main()