
# -*- coding: utf-8 -*-

import util
from pyspark.sql.functions import *
from pyspark.sql.types import *


class EgoNetwork(object):
    """

    """

    def __init__(self, edges, sc=None, sqlContext=None):

        self.sc = sc
        self.sqlContext = sqlContext

        # - Allowed values: pyspark dataframe
        # - Contains edges as relationships between objects with columns (src,dst,weight)
        self.edges = edges
        self.vertices = util.create_vertices(edges, "src", "dst")

        # - Allowed values: python list or pyspark dataframe
        # - Collection of nodes that is center of network
        self.egos = self.vertices

    def set_ego_nodes(self, list_or_dataframe):
        """
        Set egos
        :param dataframe: pyspark dataframe with column 'id' or python list
        :return:
        """

        ego_dataframe = list_or_dataframe

        # Convert list to dataframe
        if type(list_or_dataframe) == list:
            rdd_list = self.sc.parallelize(list_or_dataframe)
            row_rdd_list = rdd_list.map(lambda x: Row(x))
            field_list = [StructField("id", LongType(), True)]
            schema_list = StructType(field_list)
            ego_dataframe = self.sqlContext.createDataFrame(row_rdd_list, schema_list)

        self.egos = ego_dataframe

    def random_ego_nodes(self, n_nodes):
        """
        Randomly set egos
        :param n_nodes: int, number of nodes to set as ego
        :return:
        """

        # Randomly get n_nodes number of nodes
        random_sample = self.vertices.rdd.takeSample(False, n_nodes)
        randomly_infected_nodes = self.sqlContext.createDataFrame(random_sample)

        print randomly_infected_nodes

        # Set egos
        self.set_ego_nodes(randomly_infected_nodes)

    def get_altercomm(self):
        return None

    def create_egonetworks(self, degree=1, get_altercomm=None):

        ego_neighbors = (self.egos).withColumn("ego_temp", col("Id"))
        print "nicole"
        print ego_neighbors.show()

        for i in range(0, degree, 1):

            prev_ego = ego_neighbors.selectExpr("ego_temp as Id")
            prev_neighbors = neighbors_df.selectExpr("neighbors as Id")
            ego_step = prev_ego.unionAll(prev_neighbors)
            ego_step_clean = ego_step.na.drop()

            neighbors = util.get_neighbors(self.edges, ego_step_clean)
            neighbors_df = neighbors

        return None