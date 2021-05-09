from src.rdf_data_citation._helper import template_path

from datetime import datetime
import rdflib
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Variable
from rdflib.paths import SequencePath
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import re

q1 = open("test_query1.txt", "r").read()
q2 = open("test_query2.txt", "r").read()


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
        if isinstance(node, CompValue):
            print(node.name)
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


def to_sparql_query_text(query: str = None):
    p = parser
    query_tree = p.parseQuery(query)
    query_algebra = algebra.translateQuery(query_tree)

    def overwrite(text):
        file = open("query.txt", "w+")
        file.write(text)
        file.close()

    def replace(old, new):
        # Read in the file
        with open('query.txt', 'r') as file:
            filedata = file.read()

        # Replace the target string
        filedata = filedata.replace(old, new, 1)

        # Write the file out again
        with open('query.txt', 'w') as file:
            file.write(filedata)

    def sparql_query_text(node):
        if isinstance(node, CompValue):
            if node.name == "SelectQuery":
                overwrite("{inner_node}")
            if node.name == "Project":
                PVs = " ".join(elem.n3() for elem in node.get('PV'))
                replace("{inner_node}", "select {vars} {brackets}".format(vars=PVs, brackets="{{inner_node}}"))
            if node.name == "Extend":
                if isinstance(node.get('expr'), Expr):
                    if node.get('expr').name.endswith('CONCAT'):
                        alias = node.get('var').n3()
                        expr = '(CONCAT({vars}) as {var})' \
                            .format(vars=", ".join(elem.n3() for elem in node.get('expr').arg),
                                    var=alias)
                        replace(alias, expr)
                    # TODO: Cover other expressions
                if isinstance(node.get('expr'), Variable):
                    alias = node.get('var').n3()
                    expr = '({var} as {alias})'.format(var=node.get('expr').n3(), alias=alias)
                    replace(alias, expr)
            if node.name == "Filter":
                replace("{inner_node}", "filter({inner_node}){inner_node}")
            if node.name == "RelationalExpression":
                expr = node.expr.n3()
                op = node.op
                other = node.other.n3()
                condition = "{left} {operator} {right}".format(left=expr, operator=op, right=other)
                print(condition)
                replace("{inner_node}", condition)
            if node.name == "ConditionalAndExpression":
                inner_nodes = " && ".join(["{inner_node}" for expr in node.other if isinstance(expr, Expr)])
                replace("{inner_node}", "{inner_node} && " + inner_nodes)
            if node.name == "ConditionalOrExpression":
                inner_nodes = " || ".join(["{inner_node}" for expr in node.other if isinstance(expr, Expr)])
                replace("{inner_node}", "({inner_node} || " + inner_nodes + ")")
            if node.name == "BGP":
                triples = "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                  for triple in node.triples)
                replace("{inner_node}", triples)
            if node.name == "ToMultiSet":
                replace("{inner_node}", "{" + "{inner_node}" + "}")
            if node.name == "Union":
                replace("{inner_node}", "{{inner_node}}union{{inner_node}}")  # if there was an union already before
            if node.name == "Join":
                replace("{inner_node}", "{inner_node}{inner_node}")  # if there was an union already before

    algebra.traverse(query_algebra.algebra, sparql_query_text)
    algebra.pprintAlgebra(query_algebra)


to_sparql_query_text(q1)
# to_sparql_query_text(q2)

query = open("query.txt", "r").read()
p = '{'
q = "}"
i = 0
f = 1

for e in query:
    if e in p:
        f or print()
        print(' '*i+e)
        i += 4
        f = 1
    elif e in q:
        f or print()
        i -= 4
        f = 1
        print(' '*i+e)
    else:
        not f or print(' '*i, end='')
        f = print(e, end='')

