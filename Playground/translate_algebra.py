import rdflib
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Identifier
from rdflib.paths import Path, SequencePath, NegatedPath, AlternativePath, MulPath, ZeroOrOne, ZeroOrMore, OneOrMore, \
    InvPath
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
import collections


class ExpressionNotCoveredException(Exception):
    pass


def _translate_algebra(query_algebra: rdflib.plugins.sparql.sparql.Query = None) -> str:
    """

    :param query_algebra: An algebra returned by the function call algebra.translateQuery(parse_tree)
    from the rdflib library.
    :return: The query form of the algebra.
    """

    def overwrite(text):
        file = open("query.txt", "w+")
        file.write(text)
        file.close()

    def replace(old, new, search_from_match: str = None, search_from_match_occurrence: int = None, count: int = 1):
        # Read in the file
        with open('query.txt', 'r') as file:
            query = file.read()

        def find_nth(haystack, needle, n):
            start = haystack.lower().find(needle)
            while start >= 0 and n > 1:
                start = haystack.lower().find(needle, start + len(needle))
                n -= 1
            return start

        if search_from_match and search_from_match_occurrence:
            position = find_nth(query, search_from_match, search_from_match_occurrence)
            query_pre = query[:position]
            query_post = query[position:].replace(old, new, count)
            query = query_pre + query_post
        else:
            query = query.replace(old, new, count)

        # Write the file out again
        with open('query.txt', 'w') as file:
            file.write(query)

    def convert_node_arg(node_arg):
        if isinstance(node_arg, Identifier):
            if node_arg in aggr_vars.keys():
                grp_var = aggr_vars[node_arg].pop(0)
                return grp_var.n3()
            else:
                return node_arg.n3()
        elif isinstance(node_arg, CompValue):
            return "{" + node_arg.name + "}"
        elif isinstance(node_arg, Expr):
            return "{" + node_arg.name + "}"
        elif isinstance(node_arg, str):
            return node_arg
        else:
            raise ExpressionNotCoveredException(
                "The expression {0} might not be covered yet.".format(node_arg))

    aggr_vars = collections.defaultdict(list)

    def sparql_query_text(node):
        """
         https://www.w3.org/TR/sparql11-query/#sparqlSyntax

        :param node:
        :return:
        """

        if isinstance(node, CompValue):
            # 18.2 Query Forms
            if node.name == "SelectQuery":
                overwrite("-*-SELECT-*- " + "{" + node.p.name + "}")

            # 18.2 Graph Patterns
            elif node.name == "BGP":
                # Identifiers or Paths
                # Negated path throws a type error. Probably n3() method of negated paths should be fixed
                triples = "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                  for triple in node.triples)
                replace("{BGP}", triples)
                # The dummy -*-SELECT-*- is placed during a SelectQuery or Multiset pattern in order to be able
                # to match extended variables in a specific Select-clause (see "Extend" below)
                replace("-*-SELECT-*-", "SELECT", count=-1)
                # If there is no "Group By" clause the placeholder will simply be deleted. Otherwise there will be
                # no matching {GroupBy} placeholder because it has already been replaced by "group by variables"
                replace("{GroupBy}", "", count=-1)
                replace("{Having}", "", count=-1)
            elif node.name == "Join":
                replace("{Join}", "{" + node.p1.name + "}{" + node.p2.name + "}")  #
            elif node.name == "LeftJoin":
                replace("{LeftJoin}", "{" + node.p1.name + "}OPTIONAL{{" + node.p2.name + "}}")
            elif node.name == "Filter":
                if isinstance(node.expr, CompValue):
                    expr = node.expr.name
                else:
                    raise ExpressionNotCoveredException("This expression might not be covered yet.")
                if node.p:
                    # Filter with p=AggregateJoin = Having
                    if node.p.name == "AggregateJoin":
                        replace("{Filter}", "{" + node.p.name + "}")
                        replace("{Having}", "HAVING({" + expr + "})")
                    else:
                        replace("{Filter}", "FILTER({" + expr + "}) {" + node.p.name + "}")
                else:
                    replace("{Filter}", "FILTER({" + expr + "})")

            elif node.name == "Union":
                replace("{Union}", "{{" + node.p1.name + "}}UNION{{" + node.p2.name + "}}")
            elif node.name == "Graph":
                expr = "GRAPH " + node.term.n3() + " {{" + node.p.name + "}}"
                replace("{Graph}", expr)
            elif node.name == "Extend":
                query_string = open('query.txt', 'r').read().lower()
                select_occurrences = query_string.count('-*-select-*-')
                replace(node.var.n3(), "(" + convert_node_arg(node.expr) + " as " + node.var.n3() + ")",
                        search_from_match='-*-select-*-', search_from_match_occurrence=select_occurrences)
                replace("{Extend}", "{" + node.p.name + "}")
            elif node.name == "Minus":
                expr = "{" + node.p1.name + "}MINUS{{" + node.p2.name + "}}"
                replace("{Minus}", expr)
            elif node.name == "Group":
                group_by_vars = []
                if node.expr:
                    for var in node.expr:
                        if isinstance(var, Identifier):
                            group_by_vars.append(var.n3())
                        else:
                            raise ExpressionNotCoveredException("This expression might not be covered yet.")
                    replace("{Group}", "{" + node.p.name + "}")
                    replace("{GroupBy}", "GROUP BY " + " ".join(group_by_vars) + " ")
                else:
                    replace("{Group}", "{" + node.p.name + "}")
            elif node.name == "AggregateJoin":
                replace("{AggregateJoin}", "{" + node.p.name + "}")
                for agg_func in node.A:
                    if isinstance(agg_func.res, Identifier):
                        identifier = agg_func.res.n3()
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")

                    aggr_vars[agg_func.res].append(agg_func.vars)
                    agg_func_name = agg_func.name.split('_')[1]
                    distinct = ""
                    if agg_func.distinct:
                        distinct = agg_func.distinct + " "
                    if agg_func_name == 'GroupConcat':
                        replace(identifier, "GROUP_CONCAT" + "(" + distinct
                                + agg_func.vars.n3() + ";SEPARATOR=" + agg_func.separator.n3() + ")")
                    else:
                        replace(identifier, agg_func_name.upper() + "(" + distinct
                                + convert_node_arg(agg_func.vars) + ")")
                    # For non-aggregated variables the aggregation function "sample" is automatically assigned.
                    # However, we do not want to have "sample" wrapped around non-aggregated variables. That is
                    # why we replace it. If "sample" is used on purpose it will not be replaced as the alias
                    # must be different from the variable in this case.
                    replace("(SAMPLE({0}) as {0})".format(convert_node_arg(agg_func.vars)),
                            convert_node_arg(agg_func.vars))
            elif node.name == "GroupGraphPatternSub":
                replace("GroupGraphPatternSub", " ".join([convert_node_arg(pattern) for pattern in node.part]))
            elif node.name == "TriplesBlock":
                replace("{TriplesBlock}", "".join(triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3() + "."
                                                  for triple in node.triples))

            # 18.2 Solution modifiers
            elif node.name == "ToList":
                raise ExpressionNotCoveredException("This expression might not be covered yet.")
            elif node.name == "OrderBy":
                order_conditions = []
                for c in node.expr:
                    if isinstance(c.expr, Identifier):
                        var = c.expr.n3()
                        if c.order is not None:
                            cond = c.order + "(" + var + ")"
                        else:
                            cond = var
                        order_conditions.append(cond)
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                replace("{OrderBy}", "{" + node.p.name + "}")
                replace("{OrderConditions}", " ".join(order_conditions) + " ")
            elif node.name == "Project":
                project_variables = []
                for var in node.PV:
                    if isinstance(var, Identifier):
                        project_variables.append(var.n3())
                    else:
                        raise ExpressionNotCoveredException("This expression might not be covered yet.")
                order_by_pattern = ""
                if node.p.name == "OrderBy":
                    order_by_pattern = "ORDER BY {OrderConditions}"
                replace("{Project}", " ".join(project_variables) + "{{" + node.p.name + "}}"
                        + "{GroupBy}" + order_by_pattern + "{Having}")
            elif node.name == "Distinct":
                replace("{Distinct}", "DISTINCT {" + node.p.name + "}")
            elif node.name == "Reduced":
                replace("{Reduced}", "REDUCED {" + node.p.name + "}")
            elif node.name == "Slice":
                slice = "OFFSET " + str(node.start) + " LIMIT " + str(node.length)
                replace("{Slice}", "{" + node.p.name + "}" + slice)
            elif node.name == "ToMultiSet":
                if node.p.name == "values":
                    replace("{ToMultiSet}", "{{" + node.p.name + "}}")
                else:
                    replace("{ToMultiSet}", "{-*-SELECT-*- " + "{" + node.p.name + "}" + "}")

            # 18.2 Property Path

            # 17 Expressions and Testing Values
            # # 17.3 Operator Mapping
            elif node.name == "RelationalExpression":
                expr = convert_node_arg(node.expr)
                op = node.op
                if isinstance(list, type(node.other)):
                    other = "(" + ", ".join(convert_node_arg(expr) for expr in node.other) + ")"
                else:
                    other = convert_node_arg(node.other)
                condition = "{left} {operator} {right}".format(left=expr, operator=op, right=other)
                replace("{RelationalExpression}", condition)
            elif node.name == "ConditionalAndExpression":
                inner_nodes = " && ".join([convert_node_arg(expr) for expr in node.other])
                replace("{ConditionalAndExpression}", convert_node_arg(node.expr) + " && " + inner_nodes)
            elif node.name == "ConditionalOrExpression":
                inner_nodes = " || ".join([convert_node_arg(expr) for expr in node.other])
                replace("{ConditionalOrExpression}", "(" + convert_node_arg(node.expr) + " || " + inner_nodes + ")")
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
                # The node's name which we get with node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rExistsFunc
                # ExistsFunc can only have a GroupGraphPattern as parameter. However, when we print the query algebra
                # we get a GroupGraphPatternSub
                replace("{Builtin_EXISTS}", "EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            elif node.name.endswith('Builtin_NOTEXISTS'):
                # The node's name which we get with node.graph.name returns "Join" instead of GroupGraphPatternSub
                # According to https://www.w3.org/TR/2013/REC-sparql11-query-20130321/#rNotExistsFunc
                # NotExistsFunc can only have a GroupGraphPattern as parameter. However, when we print the query algebra
                # we get a GroupGraphPatternSub
                replace("{Builtin_NOTEXISTS}", "NOT EXISTS " + "{{" + node.graph.name + "}}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            # # # # 17.4.1.5 logical-or: Covered in "RelationalExpression"
            # # # # 17.4.1.6 logical-and: Covered in "RelationalExpression"
            # # # # 17.4.1.7 RDFterm-equal: Covered in "RelationalExpression"
            elif node.name.endswith('sameTerm'):
                replace("{Builtin_sameTerm}", "SAMETERM(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            # # # # IN: Covered in "RelationalExpression"
            # # # # NOT IN: Covered in "RelationalExpression"

            # # # 17.4.2 Functions on RDF Terms
            elif node.name.endswith('Builtin_isIRI'):
                replace("{Builtin_isIRI}", "isIRI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isBLANK'):
                replace("{Builtin_isBLANK}", "isBLANK(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isLITERAL'):
                replace("{Builtin_isLITERAL}", "isLITERAL(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_isNUMERIC'):
                replace("{Builtin_isNUMERIC}", "isNUMERIC(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_STR'):
                replace("{Builtin_STR}", "STR(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_LANG'):
                replace("{Builtin_LANG}", "LANG(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_DATATYPE'):
                replace("{Builtin_DATATYPE}", "DATATYPE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_IRI'):
                replace("{Builtin_IRI}", "IRI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_BNODE'):
                replace("{Builtin_BNODE}", "BNODE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('STRDT'):
                replace("{Builtin_STRDT}", "STRDT(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRLANG'):
                replace("{Builtin_STRLANG}", "STRLANG(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_UUID'):
                replace("{Builtin_UUID}", "UUID()")
            elif node.name.endswith('Builtin_STRUUID'):
                replace("{Builtin_STRUUID}", "STRUUID()")

            # # # 17.4.3 Functions on Strings
            elif node.name.endswith('Builtin_STRLEN'):
                replace("{Builtin_STRLEN}", "STRLEN(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_SUBSTR'):
                args = [convert_node_arg(node.arg), node.start]
                if node.length:
                    args.append(node.length)
                expr = "SUBSTR(" + ", ".join(args) + ")"
                replace("{Builtin_SUBSTR}", expr)
            elif node.name.endswith('Builtin_UCASE'):
                replace("{Builtin_UCASE}", "UCASE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_LCASE'):
                replace("{Builtin_LCASE}", "LCASE(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_STRSTARTS'):
                replace("{Builtin_STRSTARTS}", "STRSTARTS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRENDS'):
                replace("{Builtin_STRENDS}", "STRENDS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_CONTAINS'):
                replace("{Builtin_CONTAINS}", "CONTAINS(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRBEFORE'):
                replace("{Builtin_STRBEFORE}", "STRBEFORE(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_STRAFTER'):
                replace("{Builtin_STRAFTER}", "STRAFTER(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('Builtin_ENCODE_FOR_URI'):
                replace("{Builtin_ENCODE_FOR_URI}", "ENCODE_FOR_URI(" + convert_node_arg(node.arg) + ")")
            elif node.name.endswith('Builtin_CONCAT'):
                expr = 'CONCAT({vars})'.format(vars=", ".join(convert_node_arg(elem) for elem in node.arg))
                replace("{Builtin_CONCAT}", expr)
            elif node.name.endswith('Builtin_LANGMATCHES'):
                replace("{Builtin_LANGMATCHES}", "LANGMATCHES(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('REGEX'):
                args = [convert_node_arg(node.text), convert_node_arg(node.pattern)]
                expr = "REGEX(" + ", ".join(args) + ")"
                replace("{Builtin_REGEX}", expr)
            elif node.name.endswith('REPLACE'):
                replace("{Builtin_REPLACE}", "REPLACE(" + convert_node_arg(node.arg)
                        + ", " + convert_node_arg(node.pattern) + ", " + convert_node_arg(node.replacement) + ")")

            # # # 17.4.4 Functions on Numerics
            elif node.name == 'Builtin_ABS':
                replace("{Builtin_ABS}", "ABS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_ROUND':
                replace("{Builtin_ROUND}", "ROUND(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_CEIL':
                replace("{Builtin_CEIL}", "CEIL(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_FLOOR':
                replace("{Builtin_FLOOR}", "FLOOR(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_RAND':
                replace("{Builtin_RAND}", "RAND()")

            # # # 17.4.5 Functions on Dates and Times
            elif node.name == 'Builtin_NOW':
                replace("{Builtin_NOW}", "NOW()")
            elif node.name == 'Builtin_YEAR':
                replace("{Builtin_YEAR}", "YEAR(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_MONTH':
                replace("{Builtin_MONTH}", "MONTH(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_DAY':
                replace("{Builtin_DAY}", "DAY(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_HOURS':
                replace("{Builtin_HOURS}", "HOURS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_MINUTES':
                replace("{Builtin_MINUTES}", "MINUTES(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SECONDS':
                replace("{Builtin_SECONDS}", "SECONDS(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_TIMEZONE':
                replace("{Builtin_TIMEZONE}", "TIMEZONE(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_TZ':
                replace("{Builtin_TZ}", "TZ(" + convert_node_arg(node.arg) + ")")

            # # # 17.4.6 Hash functions
            elif node.name == 'Builtin_MD5':
                replace("{Builtin_MD5}", "MD5(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA1':
                replace("{Builtin_SHA1}", "SHA1(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA256':
                replace("{Builtin_SHA256}", "SHA256(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA384':
                replace("{Builtin_SHA384}", "SHA384(" + convert_node_arg(node.arg) + ")")
            elif node.name == 'Builtin_SHA512':
                replace("{Builtin_SHA512}", "SHA512(" + convert_node_arg(node.arg) + ")")

            # Other
            elif node.name == 'values':
                columns = []
                for key in node.res[0].keys():
                    if isinstance(key, Identifier):
                        columns.append(key.n3())
                    else:
                        raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(key))
                values = "VALUES (" + " ".join(columns) + ")"

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
            elif node.name == 'ServiceGraphPattern':
                replace("{ServiceGraphPattern}", "SERVICE " + convert_node_arg(node.term)
                        + "{" + node.graph.name + "}")
                algebra.traverse(node.graph, visitPre=sparql_query_text)
                return node.graph
            # else:
            #     raise ExpressionNotCoveredException("The expression {0} might not be covered yet.".format(node.name))

    algebra.traverse(query_algebra.algebra, visitPre=sparql_query_text)
    query_from_algebra = open("query.txt", "r").read()
    # os.remove("query.txt")

    return query_from_algebra


q1 = open("test_query.txt", "r").read()
query_tree = parser.parseQuery(q1)
query_algebra = algebra.translateQuery(query_tree)
_translate_algebra(query_algebra)
# algebra.pprintAlgebra(query_algebra)

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

