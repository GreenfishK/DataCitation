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
q3 = open("test_query3.txt", "r").read()
q4 = open("test_query4.txt", "r").read()
q5 = open("test_query5.txt", "r").read()
q6 = open("test_query6.txt", "r").read()
q7 = open("test_query7.txt", "r").read()
q8 = open("test_query8.txt", "r").read()
q9 = open("test_query9.txt", "r").read()
q10 = open("test_query10.txt", "r").read()
q11 = open("test_query11.txt", "r").read()


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


class ExpressionNotCoveredException(Exception):
    pass


def to_sparql_query_text(query: str = None):
    p = parser
    query_tree = p.parseQuery(query)
    query_algebra = algebra.translateQuery(query_tree)

    def overwrite(text):
        file = open("query.txt", "w+")
        file.write(text)
        file.close()

    def replace(old, new, on_match: int = 1):
        # Read in the file
        with open('query.txt', 'r') as file:
            filedata = file.read()

        # Replace the target string
        def nth_repl(s, sub, repl, n):
            find = s.find(sub)
            # If find is not -1 we have found at least one match for the substring
            i = find != -1
            # loop util we find the nth or we find no match
            while find != -1 and i != n:
                # find + 1 means we start searching from after the last match
                find = s.find(sub, find + 1)
                i += 1
            # If i is equal to n we found nth match so replace
            if i == n:
                return s[:find] + repl + s[find + len(sub):]
            return s

        filedata = nth_repl(filedata, old, new, on_match)
        #filedata = filedata.replace(old, new, on_match)

        # Write the file out again
        with open('query.txt', 'w') as file:
            file.write(filedata)

    def sparql_query_text(node):
        """
         https://www.w3.org/TR/sparql11-query/#sparqlSyntax

        :param node:
        :return:
        """
        if isinstance(node, CompValue):
            # 18.2 Query Forms
            if node.name == "SelectQuery":
                if isinstance(node.p, CompValue):
                    #PVs = " ".join(elem.n3() for elem in node.PV)
                    #overwrite("Select " + PVs + " {{" + node.p.name + "}} ")
                    overwrite("Select " + "{" + node.p.name + "}")
                else:
                    raise ExpressionNotCoveredException("This object type might not be covered yet.")

            # 18.2 Graph Pattern
            if node.name == "BGP":
                triples = "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                  for triple in node.triples)
                replace("{BGP}", "{" + triples + "}")
            if node.name == "Join":
                replace("{Join}", "{" + node.p1.name + "}{" + node.p2.name + "}")  # if there was an union already before
            if node.name == "LeftJoin":
                replace("{LeftJoin}", "{" + node.p1.name + "}OPTIONAL{{" + node.p2.name + "}}")  # if there was an union already before
            if node.name == "Filter":
                expr = ""
                if isinstance(node.expr, Expr):
                    expr = node.expr.name
                else:
                    raise ExpressionNotCoveredException("This object type might not be covered yet.")
                replace("{Filter}", "{" + node.p.name + "} filter({" + expr + "})")
            if node.name == "Union":
                replace("{Union}", "{{" + node.p1.name + "}}union{{" + node.p2.name + "}}")
            if node.name == "Graph":
                expr = "graph " + node.term.n3() + " {{" + node.p.name + "}}"
                replace("{Graph}", expr)
            if node.name == "Extend":
                expr = ""

                if isinstance(node.expr, Expr):
                    replace(node.var.n3(), "(" + node.expr.name + " as " + node.var.n3() + ")")
                if isinstance(node.expr, Variable):
                    replace(node.var.n3(), "(" + node.expr.n3() + " as " + node.var.n3() + ")")
                else:
                    raise ExpressionNotCoveredException("This object type might not be covered yet.")

                replace("{Extend}", "{" + node.p.name + "}")
            if node.name == "Minus":
                expr = "{" + node.p1.name + "} minus {{" + node.p2.name + "}}"
                replace("{Minus}", expr)
            if node.name == "Group":
                group_by_vars = " ".join(var.n3() for var in node.expr if isinstance(var, Variable))
                replace("{Group}", "{" + node.p.name + "}" + "" + "group by " + group_by_vars)
            if node.name == "Aggregation":
                pass
            if node.name == "AggregateJoin":
                replace("{AggregateJoin}", "{" + node.p.name + "}")
                for agg_func in node.A:
                    if isinstance(agg_func.res, Variable):
                        placeholder = agg_func.res.n3()
                    else:
                        raise ExpressionNotCoveredException("This object type might not be covered yet.")
                    agg_func_name = agg_func.name.split('_')[1]
                    replace(placeholder, agg_func_name + "(" + agg_func.vars.n3() + ")", 1)

            # 18.2 Solution modifiers
            if node.name == "ToList":
                pass
            if node.name == "OrderBy":
                pass
            if node.name == "Project":
                PVs = " ".join(elem.n3() for elem in node.PV)
                replace("{Project}", PVs + " {" + node.p.name + "} ")
            if node.name == "Distinct":
                pass
            if node.name == "Reduced":
                pass
            if node.name == "Slice":
                pass
            if node.name == "ToMultiSet":
                replace("{ToMultiSet}", "{Select " + "{" + node.p.name + "}" + "}")

            # 18.2 Property Path

            # 17.3 Operator Mapping
            if node.name == "RelationalExpression":
                expr = node.expr.n3()
                op = node.op
                other = node.other.n3()
                condition = "{left} {operator} {right}".format(left=expr, operator=op, right=other)
                replace("{RelationalExpression}", condition)
            if node.name == "ConditionalAndExpression":
                inner_nodes = " && ".join(["{" + expr.name + "}" for expr in node.other if isinstance(expr, Expr)])
                replace("{ConditionalAndExpression}", "{" + node.expr.name + "}" + " && " + inner_nodes)
            if node.name == "ConditionalOrExpression":
                inner_nodes = " || ".join(["{" + expr.name + "}" for expr in node.other if isinstance(expr, Expr)])
                replace("{ConditionalOrExpression}", "(" + "{" + node.expr.name + "}" + " || " + inner_nodes + ")")

            # 17.4.3 Functions on Strings
            if node.name.endswith('CONCAT'):
                expr = 'concat({vars})'.format(vars=", ".join(elem.n3() for elem in node.arg))
                replace("{Builtin_CONCAT}", expr)
            if node.name.endswith('REGEX'):
                expr = "regex(" + node.text.n3() + ", " + node.pattern.n3() + ")"
                replace("{Builtin_REGEX}", expr)
            if node.name.endswith('SUBSTR'):
                expr = "substr(" + node.arg.n3() + ", " + node.start + ", " + node.length + ")"
                replace("{Builtin_SUBSTR}", expr)

    algebra.traverse(query_algebra.algebra, sparql_query_text)
    algebra.pprintAlgebra(query_algebra)


# to_sparql_query_text(q1)
# to_sparql_query_text(q2)
# to_sparql_query_text(q3)
# to_sparql_query_text(q4)
# to_sparql_query_text(q5)
# to_sparql_query_text(q6)
# to_sparql_query_text(q7)
# to_sparql_query_text(q8)
# to_sparql_query_text(q9)
# to_sparql_query_text(q10)
to_sparql_query_text(q11)

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

