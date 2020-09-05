from rdflib.plugins.stores import sparqlstore
from rdflib import Graph


def init_Connection_Graph_DB(query_endpoint, update_endpoint) -> sparqlstore.SPARQLUpdateStore:
    """
    initializes the connection to a remote GraphDB store.
    """

    store = sparqlstore.SPARQLUpdateStore()
    store.open((query_endpoint, update_endpoint))  # query endpoint and update endpoint

    #g = Graph()
    #store.add_graph()
    return store


# Only if the graph is a named graph
# g_identifier = URIRef('http://example.org/owlim#')
# graphdb_graph = Graph(store, identifier=g_identifier)



