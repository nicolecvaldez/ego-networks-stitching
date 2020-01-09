# -*- coding: utf-8 -*-


from dateutil.relativedelta import relativedelta
import py4j
import pyspark
from pyspark import SparkConf, SparkContext, SparkFiles
from pyspark.sql import SQLContext, HiveContext
from pyspark import SparkFiles
from StringIO import StringIO
from pyspark.storagelevel import StorageLevel
from pyspark.sql.functions import *
from pyspark.sql.types import *


def merge_list(list1, list2, id):
    final_list = list(set(list1+list2))
    return [i for i in final_list if i!=id]
merge_list_udf = udf(merge_list, ArrayType(LongType()))

def create_vertices(edges, src_col, dst_col):
    """
    From graph edges data, create list of vertices.

    :param edges: pyspark dataframe, edge data containing source, destination and weight columns
    :param src_col: str, source column name in data
    :param dst_col: str, destination column name in data

    :return: pyspark dataframe, containing vertices data with column name : "id"
    """

    data_1 = edges.select(src_col)
    data_2 = edges.select(dst_col)
    vertices = data_1.unionAll(data_2).dropDuplicates()
    exp = "%s as id" %(src_col)

    return vertices.selectExpr(exp)

def get_neighbors(edges, nodes):

    network_1 = (nodes.join(edges, nodes["Id"] == edges["src"], "inner")).selectExpr("Id as Id", "dst as neighbors")
    network_2 = (nodes.join(edges, nodes["Id"] == edges["dst"], "inner")).selectExpr("Id as Id", "src as neighbors")
    network = network_1.unionAll(network_2)

    return network

def get_neighbors_group(edges, nodes):

    network_1 = nodes.join(edges, nodes["Id"] == edges["src"], "inner")
    network_2 = nodes.join(edges, nodes["Id"] == edges["dst"], "inner")
    network = network_1.unionAll(network_2)

    list1 = network.groupby("Id").agg(collect_set('src').alias("src_list"), collect_set('dst').alias("dst_list"))
    list2 = list1.withColumn("neighbors", merge_list_udf("src_list", "dst_list", "Id"))

    return list2.select("Id", "neighbors")

def set_spark_context(rundate, appname):
    conf = SparkConf().\
            setAppName(appname + " " + str(rundate)).\
            set('spark.hadoop.mapreduce.output.fileoutputformat.compress', 'false').\
            set('spark.sql.parquet.compression.codec','uncompressed')
    sc = SparkContext(conf=conf)
    try:
        sc._jvm.org.apache.hadoop.hive.conf.HiveConf()
        sqlCtx = sqlContext = HiveContext(sc)
    except py4j.protocol.Py4JError:
        sqlCtx = sqlContext = SQLContext(sc)
    return sc, sqlContext