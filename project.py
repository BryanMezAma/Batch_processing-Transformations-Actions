"""
project.py
Reads a Parquet file (or folder) and writes a debug report (schema + sample
records) as a single text file. All helpers live in spark_utils.py.

Usage (EMR step / spark-submit):
    spark-submit --py-files s3://my-bucket/src/spark_utils.py \
        s3://my-bucket/src/project.py \
        --input  s3://my-bucket/data/project/ \
        --output s3://my-bucket/debug/segment_debug.txt \
        --rows 20
"""
import argparse

from spark_utils_emr import SparkUtils


def parse_args():
    parser = argparse.ArgumentParser(description="Parquet debug report")
    parser.add_argument("--input", required=True, help="Parquet file or folder")
    parser.add_argument("--output", required=True, help="Text file to write (s3://... or local path)")
    parser.add_argument("--rows", type=int, default=20, help="Number of sample records")
    parser.add_argument("--count", action="store_true", help="Also count all rows (full scan, slower)")
    return parser.parse_args()


def main():
    args = parse_args()
    su = SparkUtils("parquet-debug")

    try:
        df = su.spark.read.parquet(args.input)
        report = su.debug_report(df, title=f"Input: {args.input}",
                                 rows=args.rows, count_rows=args.count)
    except Exception as e:
        # Still leave a report behind, so a failed step can be diagnosed from S3
        su.write_text(args.output, f"Input: {args.input}\n\n=== ERROR ===\n{type(e).__name__}: {e}\n")
        su.spark.stop()
        raise

    su.write_text(args.output, report)
    print(f"Debug report written to {args.output}")
    su.spark.stop()


if __name__ == "__main__":
    main()