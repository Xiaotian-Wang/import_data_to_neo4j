from py2neo import Graph, Node, Path, Relationship
from py2neo.matching import *
import json

# neo4j Database: Address & Port
NEO4J_HOST = 'http://10.216.6.224:7474/'
# Username
NEO4J_USER_NAME = 'neo4j'
# Password
NEO4J_PASSWORD = 'test'
# Data file directory
datafile_dir = "E:/BZBK_STGX.json"

"""
Data Format JSON file
Theses are required fields, and other fields are allowed.
{
  "RECORDS": [
    {
      "ST1LX": "string",
      "ST1": "string",
      "ST2LX": "string",
      "ST2": "string",
      "GXLX": "string",
    },
    ...]
}
"""

graph = Graph(NEO4J_HOST, username=NEO4J_USER_NAME, password=NEO4J_PASSWORD)

with open(file=datafile_dir, encoding='utf8') as f:
    data = json.load(f)

data = data['RECORDS']

# i Index for printing the counts
i = 0

# Add the data to neo4j line by line.
for item in data:
    try:
        node1 = Node(item['ST1LX'], name=item['ST1'], type=item['ST1LX'])
        node2 = Node(item['ST2LX'], name=item['ST2'], type=item['ST2LX'])
        nodes1 = graph.nodes.match(name=node1['name']).all()
        no1 = len(nodes1)
        nodes2 = graph.nodes.match(name=node2['name']).all()
        no2 = len(nodes2)
        if no1 == 0:
            # If no nodes in neo4j with the same name, create it and select it as Node1. (automatically by graph.create)
            graph.create(node1)
        else:
            # If the name exists in neo4j nodes, select it as Node1.
            node1 = nodes1[0]   # 如果图数据库中有同名节点，则指定为该节点
        if no2 == 0:
            # Same operation as node1
            graph.create(node2)
        else:
            # Same operation as node1
            node2 = nodes2[0]

        # Create the relationship. If the nodes in neo4j are selected, the repeated relations will not be created.
        # (In case there are repeated data)
        relation = Relationship(node1, node2, name=item['GXLX'])
        graph.create(relation)

        # print the index for every 100 data added
        if i % 100 == 0:
            print(i)
        i += 1
    except Exception as e:
        # Write the error information and the current data index.
        with open('log.txt', 'w') as f:
            f.write(str(i) + '\n')
            f.write(str(e))
