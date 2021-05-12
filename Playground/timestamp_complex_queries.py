from src.rdf_data_citation._helper import template_path

from datetime import datetime
import rdflib
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Identifier
from rdflib.paths import Path
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
q12 = open("test_query12.txt", "r").read()
q13 = open("test_query13.txt", "r").read()
q14 = open("test_query14.txt", "r").read()
q15 = open("test_query15.txt", "r").read()
q16 = open("test_query16.txt", "r").read()
q17 = open("test_query17.txt", "r").read()
q18 = open("test_query18.txt", "r").read()
q19 = open("test_query19.txt", "r").read()
q20 = open("test_query20.txt", "r").read()
q21 = open("test_query21.txt", "r").read()
q22 = open("test_query22.txt", "r").read()
q23 = open("test_query23.txt", "r").read()
q24 = open("test_query24.txt", "r").read()
q25 = open("test_query25.txt", "r").read()
q26 = open("test_query26.txt", "r").read()
q27 = open("test_query27.txt", "r").read()
q28 = open("test_query28.txt", "r").read()


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
                if isinstance(triple[1], Path):
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

        # Write the file out again
        with open('query.txt', 'w') as file:
            file.write(filedata)

    def convert_node_arg(node_arg):
        if isinstance(node_arg, Identifier):
            return node_arg.n3()
        elif isinstance(node_arg, CompValue):
            return "{" + node_arg.name + "}"
        elif isinstance(node_arg, Expr):
            return "{" + node_arg.name + "}"
        else:
            raise ExpressionNotCoveredException(
                "The expression {0} might not be covered yet.".format(node_arg))

    def sparql_query_text(node):
        """
         https://www.w3.org/TR/sparql11-query/#sparqlSyntax

        :param node:
        :return:
        """

        if isinstance(node, CompValue):
            # 18.2 Query Forms
            if node.name == "SelectQuery":
                overwrite("SELECT " + "{" + node.p.name + "}")

            # 18.2 Graph Patterns
            elif node.name == "BGP":
                # Identifiers or Paths
                triples = "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                  for triple in node.triples)
                replace("{BGP}", "" + triples + "")
            elif node.name == "Join":
                replace("{Join}", "{" + node.p1.name + "}{" + node.p2.name + "}")  #
            elif node.name == "LeftJoin":
                replace("{LeftJoin}", "{" + node.p1.name + "}OPTIONAL{{" + node.p2.name + "}}")
            elif node.name == "Filter":
                if isinstance(node.expr, CompValue):
                    expr = node.expr.name
                else:
                    raise ExpressionNotCoveredException("This expression might not be covered yet.")
                if node.p.name == "AggregateJoin":
                    replace("{Filter}", "{" + node.p.name + "} HAVING({" + expr + "})")
                else:
                    replace("{Filter}", "{" + node.p.name + "} FILTER({" + expr + "})")
            elif node.name == "Union":
                replace("{Union}", "{{" + node.p1.name + "}}UNION{{" + node.p2.name + "}}")
            elif node.name == "Graph":
                expr = "GRAPH " + node.term.n3() + " {{" + node.p.name + "}}"
                replace("{Graph}", expr)
            elif node.name == "Extend":
                if isinstance(node.expr, Expr):
                    replace(node.var.n3(), "({" + node.expr.name + "} as " + node.var.n3() + ")")
                elif isinstance(node.expr, Identifier):
                    replace(node.var.n3(), "(" + node.expr.n3() + " as " + node.var.n3() + ")")
                else:
                    raise ExpressionNotCoveredException("This expression type {0} might "
                                                        "not be covered yet.".format(type(node.expr)))
                replace("{Extend}", "{" + node.p.name + "}")
            elif node.name == "Minus":
                expr = "{" + node.p1.name + "} MINUS {{" + node.p2.name + "}}"
                replace("{Minus}", expr)
            elif node.name == "Group":
                group_by_vars = []
                for var in node.expr:
                    if isinstance(var, Identifier):
                        group_by_vars.append(var.n3())
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                replace("{Group}", "{" + node.p.name + "}" + "" + "GROUP BY " + " ".join(group_by_vars))
            elif node.name == "AggregateJoin":
                replace("{AggregateJoin}", "{" + node.p.name + "}")
                for agg_func in node.A:
                    if isinstance(agg_func.res, Identifier):
                        placeholder = agg_func.res.n3()
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                    agg_func_name = agg_func.name.split('_')[1]
                    replace(placeholder, agg_func_name.upper() + "(" + agg_func.vars.n3() + ")", 1)
            elif node.name == 'ServiceGraphPattern':
                replace("{ServiceGraphPattern}", node.service_string)
            # elif node.name == 'GroupGraphPatternSub':
            #     # Only captures TriplesBlock but not other possible patterns of a subgraph like 'filter'
            #     # see test_query27.txt
            #     patterns = ""
            #     for pattern in node.part:
            #         if isinstance(pattern, CompValue):
            #             patterns += "{" + pattern.name + "}"
            #     replace("{GroupGraphPatternSub}", patterns)
            # elif node.name == 'TriplesBlock':
            #     triples = ""
            #     for triple in node.triples:
            #         triples += " ".join(elem.n3() for elem in triple) + "."
            #     replace("{TriplesBlock}", "{" + triples + "}")

            # 18.2 Solution modifiers
            elif node.name == "ToList":
                raise ExpressionNotCoveredException("This expression might not be covered yet.")
            elif node.name == "OrderBy":
                order_conditions = []
                for c in node.expr:
                    if isinstance(c.expr, Identifier):
                        var = c.expr.n3()
                        if c.order is not None:
                            cond = var + "(" + c.order + ")"
                        else:
                            cond = var
                        order_conditions.append(cond)
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                replace("{OrderBy}", "{" + node.p.name + "}" + "ORDER BY " + " ".join(order_conditions))
            elif node.name == "Project":
                project_variables = []
                for var in node.PV:
                    if isinstance(var, Identifier):
                        project_variables.append(var.n3())
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                replace("{Project}", " ".join(project_variables) + " {{" + node.p.name + "}} ")
            elif node.name == "Distinct":
                replace("{Distinct}", "DISTINCT {" + node.p.name + "}")
            elif node.name == "Reduced":
                replace("{Reduced}", "REDUCED {" + node.p.name + "}")
            elif node.name == "Slice":
                slice = "OFFSET " + str(node.start) + " LIMIT " + str(node.length)
                replace("{Slice}", "{" + node.p.name + "}" + slice)
            elif node.name == "ToMultiSet":
                if node.p.name == "values":
                    replace("{ToMultiSet}", "{" + node.p.name + "}")
                else:
                    replace("{ToMultiSet}", "{SELECT " + "{" + node.p.name + "}" + "}")

            # 18.2 Property Path

            # 17 Expressions and Testing Values
            # # 17.3 Operator Mapping
            elif node.name == "RelationalExpression":
                expr = convert_node_arg(node.expr)
                op = node.op
                if len(node.other) > 1:
                    other = "(" + ", ".join(convert_node_arg(expr) for expr in node.other) + ")"
                else:
                    other = convert_node_arg(node.other)
                condition = "{left} {operator} {right}".format(left=expr, operator=op, right=other)
                replace("{RelationalExpression}", condition)
            elif node.name == "ConditionalAndExpression":
                inner_nodes = " && ".join(["{" + expr.name + "}" for expr in node.other if isinstance(expr, Expr)])
                replace("{ConditionalAndExpression}", "{" + node.expr.name + "}" + " && " + inner_nodes)
            elif node.name == "ConditionalOrExpression":
                inner_nodes = " || ".join(["{" + expr.name + "}" for expr in node.other if isinstance(expr, Expr)])
                replace("{ConditionalOrExpression}", "(" + "{" + node.expr.name + "}" + " || " + inner_nodes + ")")
            elif node.name == "MultiplicativeExpression":
                left_side = convert_node_arg(node.expr)
                multiplication = left_side
                for i, operator in enumerate(node.op):
                    multiplication += operator + " " + convert_node_arg(node.other[i]) + " "
                replace("{MultiplicativeExpression}", multiplication)
            elif node.name == "AdditiveExpression":
                left_side = convert_node_arg(node.expr)
                addition = left_side
                for i, operator in enumerate(node.op):
                    addition += operator + " " + convert_node_arg(node.other[i]) + " "
                replace("{AdditiveExpression}", addition)
            elif node.name == "UnaryNot":
                replace("{UnaryNot}", "!" + convert_node_arg(node.expr))

            # # 17.4 Function Definitions
            # # # 17.4.1 Functional Forms
            elif node.name.endswith('BOUND'):
                bound_var = convert_node_arg(node.arg)
                replace("{Builtin_BOUND}", "bound(" + bound_var + ")")
            elif node.name.endswith('IF'):
                arg2 = convert_node_arg(node.arg2)
                arg3 = convert_node_arg(node.arg3)

                if_expression = "IF(" + "{" + node.arg1.name + "}, " + arg2 + ", " + arg3 + ")"
                replace("{Builtin_IF}", if_expression)
            elif node.name.endswith('COALESCE'):
                replace("{Builtin_COALESCE}", "COALESCE(" + ", ".join(convert_node_arg(arg) for arg in node.arg) + ")")
            elif node.name.endswith('Builtin_EXISTS'):
                #  node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rNotExistsFunc
                # NotExistsFunc can only have a GroupGraphPattern as parameter. However, here we get a
                # GroupGraphPatternSub
                replace("{Builtin_EXISTS}", "EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
            elif node.name.endswith('Builtin_NOTEXISTS'):
                #  node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rNotExistsFunc
                # NotExistsFunc can only have a GroupGraphPattern as parameter. However, here we get a
                # GroupGraphPatternSub
                replace("{Builtin_NOTEXISTS}", "NOT EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)

                return node.graph

            elif node.name.endswith('sameTerm'):
                replace("{Builtin_sameTerm}", "SAMETERM(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            # # # # IN
            # Covered in RelationalExpression
            # # # # NOT IN
            # Covered in RelationalExpression

            # # # 17.4.2 Functions on RDF Terms
            # # # # isIRI
            # # # # isBlank
            # # # # isLiteral
            # # # # isNumeric
            # # # # str
            # # # # lang
            # # # # datatype
            # # # # BNODE
            # # # # STRDT
            # # # # STRLANG
            # # # # UUID
            # # # # STRUUID

            # # # 17.4.3 Functions on Strings
            # # # # STRLEN
            elif node.name.endswith('SUBSTR'):
                expr = "SUBSTR(" + node.arg.n3() + ", " + node.start + ", " + node.length + ")"
                replace("{Builtin_SUBSTR}", expr)
            # # # # UCASE
            # # # # LCASE
            # # # # STRSTARTS
            # # # # STRENDS
            # # # # CONTAINS
            # # # # STRBEFORE
            # # # # STRAFTER
            # # # # ENCODE_FOR_URI
            elif node.name.endswith('CONCAT'):
                expr = 'CONCAT({vars})'.format(vars=", ".join(elem.n3() for elem in node.arg))
                replace("{Builtin_CONCAT}", expr)
            # # # # langMatches
            elif node.name.endswith('REGEX'):
                expr = "REGEX(" + node.text.n3() + ", " + node.pattern.n3() + ")"
                replace("{Builtin_REGEX}", expr)
            # # # # REPLACE

            # # # 17.4.4 Functions on Numerics
            # # # # abs
            # # # # round
            # # # # ceil
            # # # # floor
            # # # # RAND

            # # # 17.4.5 Functions on Dates and Times
            # # # # now
            # # # # year
            # # # # month
            # # # # day
            # # # # hours
            # # # # minutes
            # # # # seconds
            # # # # timezone
            # # # # tz

            # # # 17.4.6 Hash functions
            # # # # MD5
            # # # # SHA1
            # # # # SHA256
            # # # # SHA384
            # # # # SHA512

            # Other
            elif node.name == 'values':
                columns = []
                for key in node.res[0].keys():
                    if isinstance(key, Identifier):
                        columns.append(key.n3())
                    else:
                        raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(key))
                values = "VALUES (" + " ".join(columns) +")"

                rows = ""
                for elem in node.res:
                    row = []
                    for term in elem.values():
                        if isinstance(term, Identifier):
                            row.append(term.n3())  # n3() is not part of Identifier class but every subclass has it
                        elif isinstance(term, str):
                            row.append(term)
                        else:
                            raise ExpressionNotCoveredException(
                                "The expression {0} might not be covered yet.".format(term))
                    rows += "(" + " ".join(row) + ")"

                replace("values", values + "{" + rows + "}")

            else:
                pass
                #raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(node.name))

    algebra.traverse(query_algebra.algebra, visitPre=sparql_query_text)
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
# to_sparql_query_text(q11)
# to_sparql_query_text(q12)
# to_sparql_query_text(q13)
# to_sparql_query_text(q14)
# to_sparql_query_text(q15)
# to_sparql_query_text(q16)
# to_sparql_query_text(q17)
# to_sparql_query_text(q18)
# to_sparql_query_text(q19)
# to_sparql_query_text(q20)
# to_sparql_query_text(q21)
# to_sparql_query_text(q22)
# to_sparql_query_text(q23)
# to_sparql_query_text(q24)
# to_sparql_query_text(q25)
# to_sparql_query_text(q26)
# to_sparql_query_text(q27)
to_sparql_query_text(q28)

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

