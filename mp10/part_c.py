from pyspark import *
from pyspark.sql import SparkSession
from graphframes import *

sc = SparkContext()
spark = SparkSession.builder.appName('fun').getOrCreate()


def get_shortest_distances(graphframe, dst_id):
    # TODO
    # Find shortest distances in the given graphframe to the vertex which has id `dst_id`
    # The result is a dictionary where key is a vertex id and the corresponding value is
    # the distance of this node to vertex `dst_id`.
    comp = g.shortestPaths(landmarks=[dst_id]).collect()
    result_dict = {}
    for row in comp:
        #row = row.asDict()
        #print(row)
        start_point = row['id']
        # no connection
        curr_dist = -1
        # update with connection
        if dst_id in row["distances"].keys():
            curr_dist = row['distances'][dst_id]
        result_dict[start_point] = curr_dist
    return result_dict


if __name__ == "__main__":
    vertex_list = []
    edge_list = []
    with open('dataset/graph.data') as f:
        for line in f:
            src = line.split()[0]
            dst_list = line.split()[1:]
            vertex_list.append((src,))
            edge_list += [(src, dst) for dst in dst_list]

    vertices = spark.createDataFrame(vertex_list, ["id"])  # TODO: Create dataframe for vertices
    edges = spark.createDataFrame(edge_list, ["src", "dst"])  # TODO: Create dataframe for edges

    g = GraphFrame(vertices, edges)
    sc.setCheckpointDir("/tmp/shortest-paths")

    # We want the shortest distance from every vertex to vertex 1
    for k, v in get_shortest_distances(g, '1').items():
        print(k, v)
