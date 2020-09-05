'''
Execute this only once to load the nt and turtle files into a persistent graph store
which is located in the top hierarchy level of the project folder.
'''

from rdflib import Graph

g = Graph('Sleepycat', identifier='news')

# first time create the store:
g.open('graphStore', create= True)

g.parse("/opt/graphdb-free/app/examples/data/news/graphdb-news-dataset.nt", format="nt")
g.parse("/opt/graphdb-free/app/examples/data/news/pub-properties.ttl", format="turtle")
g.parse("/opt/graphdb-free/app/examples/data/news/pub-ontology-types.ttl", format="turtle")
g.parse("/opt/graphdb-free/app/examples/data/news/pub-ontology.ttl", format="turtle")
g.parse("/opt/graphdb-free/app/examples/data/news/publishing-ontology.ttl", format="turtle")

print("The graph was created and %d triples where loaded" % len(g))

# when done!
g.close()