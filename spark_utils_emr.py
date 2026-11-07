from datetime import datetime, timezone
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.types import StructType, StringType, IntegerType, LongType, ShortType, DoubleType, FloatType, BooleanType, DateType, FloatType, BooleanType, DateType, TimestampType, BinaryType, StructField, ArrayType
from pyspark.sql.functions import count, when, isnull

class SparkUtils:

    def __init__(self, app_name, master_url=None, spark_jars=None, spark_packages=None):
        spark_builder = (
            SparkSession.builder
            .appName(app_name))
        
        if master_url is not None:
            spark_builder = spark_builder.master(master_url)

        if spark_jars is not None:
            spark_builder = spark_builder.config("spark.jars", spark_jars)

        if spark_packages is not None:
            spark_builder = spark_builder.config("spark.jars.packages", spark_packages)

        self._spark = spark_builder.getOrCreate()

    @property
    def spark(self):
        return self._spark

    @staticmethod
    def generate_schema(columns_info) -> StructType:
        """
        Generates a list of StructField objects from a list of tuples.

        Args:
            column_info (list of tuples): Each tuple contains (column_name, data_type_string).

        Returns:
            list: A list of StructField objects.
        """
        # Mapping from string type names to PySpark data types
        type_mapping = {
            "string": StringType(),
            "int": IntegerType(),
            "long": LongType(),
            "short": ShortType(),
            "double": DoubleType(),
            "float": FloatType(),
            "boolean": BooleanType(),
            "date": DateType(),
            "timestamp": TimestampType(),
            "binary": BinaryType(),
            "array_int": ArrayType(IntegerType()),
            "array_string": ArrayType(StringType()),
            "struct": StructType()
        }

        struct_fields = []
        for column_info in columns_info:
            col_name  = column_info[0]
            col_type  = column_info[1]

            if col_type == "struct":
                # Expect a third element with the sub-field definitions
                if len(column_info) < 3 or not isinstance(column_info[2], list):
                    raise ValueError(
                        f"Column '{col_name}' is declared as 'struct' but "
                        f"no sub-fields list was provided as the third element."
                    )
                # Recurse to build the nested StructType
                nested_schema = SparkUtils.generate_schema(column_info[2])
                struct_field  = StructField(col_name, nested_schema, True)

            elif col_type not in type_mapping:
                raise ValueError(f"Unsupported data type: '{col_type}' for column '{col_name}'")

            else:
                # Create a StructField for the column
                struct_field = StructField(column_info[0], type_mapping[column_info[1]], True)
            
            struct_fields.append(struct_field)

        return StructType(struct_fields) 

    @staticmethod
    def count_nulls(df):
        return df.select([count(when(isnull(c), c)).alias(c) for c in df.columns])


    @staticmethod
    def schema_as_text(df) -> str:
        """Returns the same tree that df.printSchema() prints, as a string."""
        return df._jdf.schema().treeString()

    @staticmethod
    def sample_as_text(df, rows=20, truncate=80, vertical=True) -> str:
        """Returns the same table that df.show() prints, as a string."""
        return df._jdf.showString(rows, truncate, vertical)

    @staticmethod
    def sample_as_json(df, rows=20) -> list:
        """Returns the first rows as JSON strings (keeps nested structs readable)."""
        return df.limit(rows).toJSON().collect()

    def debug_report(self, df, title="", rows=20, count_rows=False) -> str:
        """Builds a text report with input files, schema and a sample of records."""
        sample = df.limit(rows)
        files = df.inputFiles()

        lines = [f"Debug report - {datetime.now(timezone.utc).isoformat()}", title, ""]

        lines.append(f"Files read: {len(files)}")
        for f in files[:10]:
            lines.append(f"  {f}")
        if len(files) > 10:
            lines.append(f"  ... and {len(files) - 10} more")
        lines.append(f"Columns: {len(df.columns)}")
        if count_rows:
            lines.append(f"Row count: {df.count():,}")  # full scan
        lines.append("")

        lines.append("=== SCHEMA ===")
        lines.append(SparkUtils.schema_as_text(df))

        lines.append(f"=== SAMPLE ({rows} rows, vertical) ===")
        lines.append(SparkUtils.sample_as_text(sample, rows))

        lines.append(f"=== SAMPLE ({rows} rows, JSON) ===")
        lines.extend(SparkUtils.sample_as_json(sample, rows))

        return "\n".join(lines) + "\n"

    def write_text(self, path, text):
        """
        Writes a single text file using Hadoop's FileSystem API.
        Works with s3://, hdfs:// and file:// paths (overwrites if it exists).
        """
        jvm = self._spark._jvm
        hadoop_path = jvm.org.apache.hadoop.fs.Path(path)
        fs = hadoop_path.getFileSystem(self._spark._jsc.hadoopConfiguration())
        stream = fs.create(hadoop_path, True)
        try:
            stream.write(bytearray(text.encode("utf-8")))
        finally:
            stream.close()

    def select_relevant_columns(self, df):
        return df.select(
            "id",
            F.col("names.primary").alias("name"),
            F.try_element_at(F.col("sources"), F.lit(1)).getField("dataset").alias("source_dataset"),
            "height", "num_floors", "subtype", F.col("class").alias("building_class"),
            "roof_shape", "is_underground", "has_parts",
            F.col("bbox.xmin").alias("xmin"), F.col("bbox.xmax").alias("xmax"),
            F.col("bbox.ymin").alias("ymin"), F.col("bbox.ymax").alias("ymax"),
        )

    def clean_buildings(self, df):
        return (
            df.filter(F.col("id").isNotNull())
              .filter(F.col("xmin").isNotNull() & F.col("ymin").isNotNull()
                      & F.col("xmax").isNotNull() & F.col("ymax").isNotNull())
              .filter(F.col("xmin").between(-180, 180) & F.col("xmax").between(-180, 180)
                      & F.col("ymin").between(-90, 90) & F.col("ymax").between(-90, 90))
              .filter(F.col("height").isNull() | ((F.col("height") > 0) & (F.col("height") < 1000)))
              .fillna({"source_dataset": "unknown"})
              .dropDuplicates(["id"])
        )

    def derive_columns(self, df):
        center_lon = (F.col("xmin") + F.col("xmax")) / 2
        center_lat = (F.col("ymin") + F.col("ymax")) / 2
        meters_per_deg = F.lit(111320.0)
        bbox_area_m2 = (
            (F.col("xmax") - F.col("xmin")) * meters_per_deg * F.cos(F.radians(center_lat))
            * (F.col("ymax") - F.col("ymin")) * meters_per_deg
        )
        return (
            df.withColumn("center_lon", center_lon)
              .withColumn("center_lat", center_lat)
              .withColumn("region",
                          F.concat(F.lit("lat"), (F.floor(center_lat / 10) * 10).cast("string"),
                                   F.lit("_lon"), (F.floor(center_lon / 10) * 10).cast("string")))
              .withColumn("bbox_area_m2", F.abs(bbox_area_m2))
              .withColumn("height_class",
                          F.when(F.col("height").isNull(), "unknown")
                           .when(F.col("height") < 10, "low")
                           .when(F.col("height") < 30, "mid")
                           .otherwise("high"))
              .withColumn("has_height", F.col("height").isNotNull().cast("int"))
              .withColumn("has_floors", F.col("num_floors").isNotNull().cast("int"))
              .withColumn("has_name", F.col("name").isNotNull().cast("int"))
              .withColumn("has_subtype", F.col("subtype").isNotNull().cast("int"))
        )

    def aggregate_by_region_source(self, df):
        return (
            df.groupBy("region", "source_dataset", "height_class")
              .agg(F.count("*").alias("n_buildings"),
                   F.avg("height").alias("avg_height_m"),
                   F.avg("bbox_area_m2").alias("avg_bbox_area_m2"),
                   F.sum("has_name").alias("n_with_name"),
                   F.sum("has_floors").alias("n_with_floors"),
                   F.sum("has_subtype").alias("n_with_subtype"))
        )

    def region_totals(self, df):
        return (
            df.groupBy("region")
              .agg(F.count("*").alias("region_total"),
                   F.avg("has_height").alias("region_pct_with_height"))
        )

    def join_with_region_totals(self, summary, totals):
        return (
            summary.join(totals, on="region", how="inner")
                   .withColumn("pct_of_region",
                               F.round(F.col("n_buildings") / F.col("region_total") * 100, 4))
                   .withColumn("pct_with_name",
                               F.round(F.col("n_with_name") / F.col("n_buildings") * 100, 2))
        )

    def filter_and_sort(self, df, min_buildings=1000):
        return (
            df.filter(F.col("n_buildings") >= min_buildings)
              .orderBy(F.desc("n_buildings"))
        )

    def write_parquet(self, df, output_path, partition_col="source_dataset"):
        (df.repartition(partition_col)
           .write.mode("overwrite")
           .partitionBy(partition_col)
           .parquet(output_path))
