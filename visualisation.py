import graphviz
import fast_step as fs
import delete_algo as da

# dot = graphviz.Digraph('round-table', comment='The Round Table')
# dot.node('A', 'King Arthur')
# dot.node('B', 'Sir Bedevere the Wise')
# dot.node('L', 'Sir Lancelot the Brave')
#
# dot.edges(['AB', 'AL'])
# dot.edge('B', 'L', constraint='true')
#
#
# dot.render(directory='doctest-output', view=True)


# dot = graphviz.Digraph('round-table', comment='neo4j graph', engine="sfdp",
#                        graph_attr={"concentrate": "true", "overlap": "prism",
#                                    "splines": "true", "overlap_shrink": "false"})

# dot = graphviz.Digraph('round-table', comment='neo4j graph', engine="sfdp",
#                        graph_attr={"concentrate": "true",
#                                    "splines": "true"})
dot = graphviz.Graph('neo4j graph', filename='process.gv',
                     graph_attr={"concentrate": "true", "overlap": "prism",
                                 "splines": "true", "overlap_shrink": "false"})
# engine='sfdp',
for element in fs.id_and_name():
    dot.node(element[0], element[0])

for element in fs.id_and_name():
    for id in fs.links(element[0], db="neo4j"):
        dot.edge(element[0], id)
# print(dot.source)
dot.render(directory='doctest-output', view=True)
#########111111

test_id = fs.test_id()

dot = graphviz.Graph('neo4j graph', filename='new_process.gv',
                     graph_attr={"concentrate": "true", "overlap": "prism",
                                 "splines": "true", "overlap_shrink": "false"})

#############
# engine='sfdp',
for element in fs.id_and_name():
    if element[0] in fs.test_id():
        dot.node(element[0], element[0], shape='rectangle', color='red')
    else:
        dot.node(element[0], element[0])

for element in fs.id_and_name():
    for id in fs.links(element[0], db="neo4j"):
        dot.edge(element[0], id)
# print(dot.source)
dot.render(directory='doctest-output', view=True)
###########22222
dot = graphviz.Graph('neo4j graph', filename='process.gv',
                     graph_attr={"concentrate": "true", "overlap": "prism",
                                 "splines": "true", "overlap_shrink": "false"})
# engine='sfdp',
for element in fs.test_id():
    # dot.node(element, element, shape='rectangle', color='red')
    dot.node(element, element, shape='rectangle', color='red')
for element in fs.test_id():
    for link in fs.links(element):
        dot.edge(element, link)

# for element in fs.test_id():
#     for id in da.links(element):
#         dot.edge(element, id)
# print(dot.source)
dot.render(directory='doctest-output', view=True)

# take after delete_algo


# https://stackoverflow.com/questions/44337180/graphviz-python-recoloring-a-single-node-after-it-has-been-generated
