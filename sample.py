import networkx as nx
import pandas as pd
from pyspark import SparkContext
from pyspark.sql import SQLContext
from pyspark.sql.functions import *
from pyspark.sql.types import *


sc =SparkContext()
sqlContext = SQLContext(sc)

nx_graph = nx.barabasi_albert_graph(1000,900)
edge_list = [e for e in nx_graph.edges()]
pd_edge = pd.DataFrame(edge_list, columns=["src", "dst"])

s_edge = sqlContext.createDataFrame(pd_edge)



from egonetwork_stitching import EgoNetwork

en = EgoNetwork(s_edge, sc, sqlContext)
en.random_ego_nodes(5)

check = en.create_egonetworks()
check.show()