"""
project_etl.py
Final project pipeline: Overture Maps buildings -> coverage / quality summary
by region and data source, persisted as partitioned Parquet.

Usage (EMR step / spark-submit):
    spark-submit --deploy-mode cluster \
        --py-files s3://ffinalprojectt/src/spark_utils_emr.py \
        s3://ffinalprojectt/src/project_etl.py \
        --input  s3://ffinalprojectt/data/project/buildings/type=building/ \
        --output s3://ffinalprojectt/output/buildings_summary/
"""
import argparse

from spark_utils_emr import SparkUtils


def parse_args():
    parser = argparse.ArgumentParser(description="Buildings coverage pipeline")
    parser.add_argument("--input", required=True, help="Parquet folder with buildings")
    parser.add_argument("--output", required=True, help="S3 output prefix")
    parser.add_argument("--min-buildings", type=int, default=1000,
                        help="Minimum buildings per group to keep")
    return parser.parse_args()


def main():
    args = parse_args()
    su = SparkUtils("buildings-pipeline")

    df = su.spark.read.parquet(args.input)

    df = su.select_relevant_columns(df)
    df = su.clean_buildings(df)
    df = su.derive_columns(df)

    summary = su.aggregate_by_region_source(df)
    totals = su.region_totals(df)
    joined = su.join_with_region_totals(summary, totals)
    result = su.filter_and_sort(joined, args.min_buildings)

    su.write_parquet(result, args.output, partition_col="source_dataset")

    print(f"Output written to {args.output}")
    su.spark.stop()


if __name__ == "__main__":
    main()
