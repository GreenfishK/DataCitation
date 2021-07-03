from rdf_data_citation.citation_utils import QueryUtils
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra

query = open("test_query.txt", "r").read()
query_utils = QueryUtils(query=query)

query_tree = parser.parseQuery(query)
query_algebra = algebra.translateQuery(query_tree)

print(query)
algebra.pprintAlgebra(query_algebra)
algebra.pprintAlgebra(query_utils.normal_query_algebra)
print(query_utils.normal_query)