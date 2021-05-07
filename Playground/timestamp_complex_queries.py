import rdflib
from rdflib.plugins.sparql.parserutils import CompValue
from src.rdf_data_citation._helper import template_path
from rdflib.paths import SequencePath
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import re

q1 = """
# Prefixes
PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
PREFIX publishing: <http://ontology.ontotext.com/publishing#>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

select ?document ?mention ?personLabel ?party_label {
    {
        select *  {
            ?document publishing:containsMention ?mention .
            ?person pub:memberOfPoliticalParty ?party .
            ?person pub:preferredLabel ?personLabel .
            ?party pub:hasValue ?value .
            ?value pub:preferredLabel ?party_label .
            filter(?personLabel = "Judy Chu"@en)
            
            {
                Select * where {
                    ?mention publishing:hasInstance ?person .
                    
                }
            }
        }
    }
    union
    {
        select * where {
            ?mention publishing:hasInstance ?person .
            ?document publishing:containsMention ?mention .
            ?person pub:memberOfPoliticalParty / pub:hasValue / pub:preferredLabel ?party_label .
            ?person pub:preferredLabel ?personLabel .
            filter(?personLabel = "Barack Obama"@en)
        }
    }
}
"""


def query_triples(query, sparql_prefixes: str = None) -> dict:
    """
    Takes a query and extracts the triple statements from it that are found in the query body.

    :return: A list of triple statements.
    """

    template = open(template_path("templates/query_utils/prefixes_query.txt"), "r").read()

    if sparql_prefixes:
        statement = template.format(sparql_prefixes, query)
    else:
        statement = template.format("", query)

    p = parser
    query_tree = p.parseQuery(statement)
    q_algebra = algebra.translateQuery(query_tree)
    n3_triple_sets = {}

    def retrieve_bgp(node):
        if isinstance(node, CompValue) and node.name == 'BGP':
            triple_set = []

            for triple in node.get('triples'):
                if isinstance(triple[1], SequencePath):
                    sequences = triple[1].args
                    for i, ref in enumerate(sequences, start=1):
                        if i == 1:
                            t = triple[0].n3() + " " + ref.n3() + " " + "?dummy{0}".format(str(i))
                        elif i == len(sequences):
                            t = "?dummy{0}".format(len(sequences) - 1) + " " + ref.n3() + " " + triple[2].n3()
                        else:
                            t = "?dummy{0}".format(str(i - 1)) + " " + ref.n3() + " " + "?dummy{0}".format(str(i))
                        triple_set.append(t)
                else:
                    t = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
                    triple_set.append(t)
            n3_triple_sets[node.name+str(len(n3_triple_sets))] = triple_set

            dummy_triple = (rdflib.term.Variable('dummy_subject_{0}'.format(node.name+str(len(n3_triple_sets)))),
                            rdflib.term.URIRef('http://dummy.dummy.com/dummy/hasValue'),
                            rdflib.term.Variable('dummy_value_{0}'.format(node.name+str(len(n3_triple_sets)))))
            node.get('triples').append(dummy_triple)
            return node

    algebra.traverse(q_algebra.algebra, retrieve_bgp)
    print(algebra.pprintAlgebra(q_algebra))

    return n3_triple_sets


n3_triple_sets = query_triples(q1)


statement = "Select ?s ?p ?o where {?s ?p ?o.}"
query_tree = parser.parseQuery(statement) # query parse-tree
q_algebra = algebra.translateQuery(query_tree) # query algebra
algebra.pprintAlgebra(q_algebra)