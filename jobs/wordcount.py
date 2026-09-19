from pyspark.sql import SparkSession
from pyspark.sql.functions import explode, split, col

def main():
    spark = SparkSession.builder \
        .appName("WordCount_김광영") \
        .getOrCreate()

    spark.sparkContext.setLogLevel("WARN")

    # 입력 파일 로드 (컨테이너 내부 경로)
    file_path = "/opt/spark/data/wordcount.txt"
    text_df = spark.read.text(file_path)

    # 전체 단어 수(total words) 계산
    # 공백만 기준으로 단어 분리 (문제지 조건 엄수: 소문자 변환/구두점 제거 금지)
    words_df = text_df.select(
        explode(split(col("value"), "\\s+")).alias("word")
    ).filter(col("word") != "")

    total_words_count = words_df.count()

    # 고유 단어 수(distinct words) 계산
    distinct_words_count = words_df.distinct().count()

    print(f"total words: {total_words_count}")
    print(f"distinct words: {distinct_words_count}")

    # 단어별 개수 집계 및 빈도 내림차순 정렬, 상위 20개 출력
    word_counts = words_df.groupBy("word") \
        .count() \
        .orderBy(col("count").desc(), col("word").asc())

    word_counts.show(20, truncate=False)

    spark.stop()

if __name__ == "__main__":
    main()