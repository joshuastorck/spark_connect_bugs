import argparse
import contextlib
import os
import pathlib
import subprocess
import tempfile

import pyspark.sql as ps


@contextlib.contextmanager
def connect_server(spark_home: pathlib.Path, port: int) -> None:
    env = dict(os.environ)
    with tempfile.TemporaryDirectory() as tmpdir:
        env["SPARK_LOG_DIR"] = tmpdir
        env["SPARK_PID_DIR"] = tmpdir
        sbin = spark_home / "sbin"
        start = sbin / "start-connect-server.sh"
        subprocess.run(
            [
                start,
                "--packages",
                "org.apache.spark:spark-connect_2.12:3.5.2",
                "--conf",
                f"spark.connect.grpc.binding.port={port}",
                "--conf",
                "spark.sql.pyspark.legacy.inferMapTypeFromFirstPair.enabled=true",
            ],
            env=env,
            check=True,
        )
        try:
            yield ps.SparkSession.builder.remote(f"sc://localhost:{port}").getOrCreate()
        finally:
            stop = sbin / "stop-connect-server.sh"
            subprocess.run([stop], env=env, check=True)


if __name__ == "__main__":
    parser = argparse.ArgumentParser()
    parser.add_argument(
        "-s",
        "--spark-home",
        default=pathlib.Path(__file__).parent / "spark-3.5.2-bin-hadoop3",
        help="Location of the Spark distribution, defaults to %(default)s",
    )
    parser.add_argument(
        "-p",
        "--port",
        default=12345,
        type=int,
        help="Port for connect server to bind to, defaults to %(default)s",
    )
    arguments = parser.parse_args()

    with connect_server(arguments.spark_home, arguments.port) as spark:
        df = spark.createDataFrame([[1], [2]], ["val"])
        # When databricks-connect is installed, the column remains 'val'
        df.withColumnRenamed("val", "val2").show()
