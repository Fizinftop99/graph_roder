import graphviz
import fast_step as fs

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
dot = graphviz.Graph('neo4j graph', filename='process.gv', engine='sfdp',
                     graph_attr={"concentrate": "true", "overlap": "prism",
                                    "splines": "true", "overlap_shrink": "false"})

for element in fs.id_and_name():
    dot.node(element[0], element[0])


for element in fs.id_and_name():
    for id in fs.links(element[0]):
        dot.edge(element[0], id)
print(dot.source)
dot.render(directory='doctest-output', view=True)




