from ._helper import template_path, versioning_timestamp_format
from .prefixes import split_prefixes_query, versioning_prefixes
from ._exceptions import MultipleSortIndexesError, NoUniqueSortIndexError, \
    ExpressionNotCoveredException, InputMissing
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Variable, Identifier, URIRef
from rdflib.paths import SequencePath, Path, NegatedPath, AlternativePath, InvPath, MulPath, ZeroOrOne, \
    ZeroOrMore, OneOrMore
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
import rdflib.plugins.sparql.parser
from rdflib.plugins.sparql.operators import TrueFilter, UnaryNot, Builtin_BOUND
import pandas as pd
from pandas.util import hash_pandas_object
import hashlib
import datetime
import json
import numpy as np
from datetime import datetime, timedelta, timezone
import tzlocal
import os
import logging
import collections


def _query_algebra(query: str, sparql_prefixes: str) -> rdflib.plugins.sparql.algebra.Query:
    """
    Takes query and prefixes as strings and generates the query tree.
    :param query:
    :param sparql_prefixes: A query tree
    :return:
    """
    if sparql_prefixes:
        # Warning: a_http://ontology.ontotext.com/taxonomy/preferredLabel_Democratic Party does
        # not look like a valid URI, trying to serialize this will break.
        # Warning gets issued because of white spaces in case a triple statement's object is a literal
        parse_tree = parser.parseQuery(sparql_prefixes + " " + query)
    else:
        parse_tree = parser.parseQuery(query)
    query_algebra = algebra.translateQuery(parse_tree)

    return query_algebra


def _pprint_query(query: str):
    p = '{'
    q = "}"
    i = 0
    f = 1

    for e in query:
        if e in p:
            f or print()
            print(' ' * i + e)
            i += 4
            f = 1
        elif e in q:
            f or print()
            i -= 4
            f = 1
            print(' ' * i + e)
        else:
            not f or print(' ' * i, end='')
            f = print(e, end='')


def _order_by_variables(query: str, prefixes: str) -> dict:
    """
    The query must be a valid query including prefixes.
    The query algebra is searched for "PV". There can be more than one PV-Nodes containing the select-clause
    variables within a list. However, each of these lists enumerates the same variables only the first list
    will be selected and returned.

    :param query: The select statement.
    :param prefixes: The query prefixes or prologue.
    :return: a list of variables used in the query
    """

    query_algebra = _query_algebra(query, prefixes)
    order_by_variables = {}

    def retrieve_order_by_variables(node):
        if isinstance(node, CompValue) and node.name == 'OrderBy':
            order_by_conditions = node.get('expr')
            order_by_vars = []
            for order_condition in order_by_conditions:
                variable = order_condition['expr']
                if isinstance(variable, Variable):
                    order_by_vars.append(variable)
                else:
                    raise ExpressionNotCoveredException("Please provide only variables in the sort by clause.")
            order_by_variables[len(order_by_variables)] = order_by_vars
            return order_by_variables

    # Traverses the query tree and extracts the variables into a list 'variables' that is defined above.
    algebra.traverse(query_algebra.algebra, retrieve_order_by_variables)

    return order_by_variables


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
            if node_arg in aggr_vars.keys() and len(aggr_vars[node_arg]) > 0:
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
    os.remove("query.txt")

    return query_from_algebra


def _resolve_paths(node: CompValue):
    if isinstance(node, CompValue):
        if node.name == "BGP":
            resolved_triples = []

            def resolve(path: Path, subj, obj):
                if isinstance(path, SequencePath):
                    for i, ref in enumerate(path.args, start=1):
                        if i == 1:
                            s = subj
                            p = ref
                            o = Variable("?dummy{0}".format(str(i)))
                        elif i == len(path.args):
                            s = Variable("?dummy{0}".format(len(path.args) - 1))
                            p = ref
                            o = obj
                        else:
                            s = Variable("?dummy{0}".format(str(i - 1)))
                            p = ref
                            o = Variable("?dummy{0}".format(str(i)))
                        if isinstance(ref, URIRef):
                            t = (s, p, o)
                            resolved_triples.append(t)
                            continue
                        if isinstance(ref, Path):
                            resolve(p, s, o)
                        else:
                            raise ExpressionNotCoveredException("Node inside Path is neither Path nor URIRef but: "
                                                                "{0}. This case has not been covered yet. "
                                                                "Path will not be resolved.".format(type(ref)))

                if isinstance(path, NegatedPath):
                    raise ExpressionNotCoveredException("NegatedPath has not be covered yet. Path will not be resolved")
                if isinstance(path, AlternativePath):
                    raise ExpressionNotCoveredException("AlternativePath has not be covered yet. "
                                                        "Path will not be resolved. Instead of alternative paths "
                                                        "try using the following expression: "
                                                        "{ <triple statements> } "
                                                        "UNION "
                                                        "{ <triple statements> }")
                if isinstance(path, InvPath):
                    if isinstance(path.arg, URIRef):
                        t = (obj, path.arg, subj)
                        resolved_triples.append(t)
                        return
                    else:
                        raise ExpressionNotCoveredException("An argument for inverted paths other than URIRef "
                                                            "was given. This case is not implemented yet.")
                if isinstance(path, MulPath):
                    if triple[1].mod == ZeroOrOne:
                        raise ExpressionNotCoveredException("ZeroOrOne path has not be covered yet. "
                                                            "Path will not be resolved")
                    if triple[1].mod == ZeroOrMore:
                        raise ExpressionNotCoveredException("ZeroOrMore path has not be covered yet. "
                                                            "Path will not be resolved")
                    if triple[1].mod == OneOrMore:
                        raise ExpressionNotCoveredException("OneOrMore path has not be covered yet. "
                                                            "Path will not be resolved")

            for k, triple in enumerate(node.triples):
                if isinstance(triple[0], Identifier) and isinstance(triple[2], Identifier):
                    if isinstance(triple[1], Path):
                        resolve(triple[1], triple[0], triple[2])
                    else:
                        if isinstance(triple[1], Identifier):
                            resolved_triples.append(triple)
                        else:
                            raise ExpressionNotCoveredException("Predicate is neither Path nor Identifier but: {0}. "
                                                                "This case has not been covered yet.".format(triple[1]))
                else:
                    raise ExpressionNotCoveredException("Subject and/or object are not identifiers but: {0} and {1}."
                                                        " This is not implemented yet.".format(triple[0], triple[2]))

            node.triples.clear()
            node.triples.extend(resolved_triples)
            node.triples = algebra.reorderTriples(node.triples)

        elif node.name == "TriplesBlock":
            raise ExpressionNotCoveredException("TriplesBlock has not been covered yet. "
                                                "If there are any paths they will not be resolved.")


class QueryUtils:

    def __init__(self, query: str = None, execution_timestamp: datetime = None):
        """
        Initializes the QueryData object and presets most of the variables by calling functions from this class.

        :param query: The SPARQL select statement that is used to retrieve the data set for citing.
        :param execution_timestamp: The timestamp as of q_handler.
        """

        if query is not None:
            self.sparql_prefixes, self.query = split_prefixes_query(query)
            try:
                self.normal_query_algebra = self.normalize_query_tree()
                self.normal_query = _translate_algebra(self.normal_query_algebra)
                self.order_by_variables = _order_by_variables(self.query, self.sparql_prefixes)
            except ExpressionNotCoveredException as e:
                logging.error(e)
                raise ExpressionNotCoveredException(e)
            self.checksum = self.compute_checksum()

            if execution_timestamp is not None:
                self.execution_timestamp = versioning_timestamp_format(execution_timestamp)  # -> str
            else:
                current_datetime = datetime.now()
                timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
                execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
                execution_timestamp = versioning_timestamp_format(execution_datetime)
                self.execution_timestamp = execution_timestamp
            self.timestamped_query = self.timestamp_query()
            self.pid = self.generate_query_pid()
        else:
            self.query = None
            self.normal_query_algebra = None
            self.checksum = None

    def normalize_query_tree(self, query: str = None) -> rdflib.plugins.sparql.algebra.Query:
        """
        R4 - Query Uniqueness
        Normalizes the query tree by computing its algebra first. Most of the ambiguities are implicitly solved by the
        query algebra as different ways of writing a query usually boil down to one algebra. For cases where
        normalization is still needed, the query algebra is be updated. First, the query is wrapped with a select
        statement selecting all its variables in unambiguous order. The query tree of the extended query
        is then computed and normalization measures are taken.

        :return: normalized query tree object
        """

        # Assertions and exception handling
        if query is None:
            if self.query is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                raise InputMissing("Query could not be normalized because the query string was not set.")
        else:
            prefixes, query = split_prefixes_query(query)
        q_algebra = _query_algebra(query, prefixes)

        def remove_protected_vars(node):
            """
            Removes protected nodes _vars from the query tree. _vars store just the used variables within the
            corresponding node and are found in every CompValue. E.g. _vars in BGP are variables encountered in triple
            statements. Excluding them does not change the query semantics in any way as they are just additional
            information.

            :return:
            """
            if isinstance(node, set):
                return set()

        algebra.traverse(q_algebra.algebra, visitPost=remove_protected_vars)

        """
        #6.1
        Aliases via BIND keyword just rename variables but the query result stays the same.
        It must be distinguished whether bind is used to give existing variables an alias or
        to do the same with more complex expressions. A normalization will only be carried out
        in former case.
        Measure: Aliases just for the purpose of giving simple variables another name will be removed.
        """
        aliases = {}

        def remove_alias(node):
            if isinstance(node, CompValue) and node.name == "Extend" and isinstance(node.expr, Variable):
                if node.expr.n3().startswith('?__agg_'):
                    aliases[node.expr] = node.var
                else:
                    aliases[node.var] = node.expr
                return node.p

        def overwrite_aliases_with_orig_var(node):
            if isinstance(node, Variable):
                if aliases.get(node):
                    return aliases.get(node)

        algebra.traverse(q_algebra.algebra, visitPost=remove_alias)
        algebra.traverse(q_algebra.algebra, visitPost=overwrite_aliases_with_orig_var)

        """
        #6.2
        Aliases that are assigned in the select clause are just another means to #6.1 (using bind to
        assign aliases). 
        Measure: Same as 6.1
        """
        # Solved with 6.1 as "Extend" in the algebra tree can mean explicit binding using the BIND keyword
        # or implicitly in the projection part (=select variables) of the select clause.

        """
        #8 
        "filter not exists {triple}" expressions will be converted into the "filter + !bound" expression.
        """

        def replace_filter_not_exists(node):
            if isinstance(node, CompValue) and node.name == 'Filter':
                if node.expr.name == 'Builtin_NOTEXISTS' and node.p.name == 'BGP':
                    if len(node.expr.graph.p2.triples) == 1:
                        return algebra.Filter(expr=Expr(name='UnaryNot', evalfn=UnaryNot,
                                                        expr=Expr(name='Builtin_BOUND',
                                                                  evalfn=Builtin_BOUND,
                                                                  arg=node.expr.graph.p2.triples[0][2])),
                                              p=algebra.LeftJoin(node.p,
                                                                 algebra.BGP(triples=node.expr.graph.p2.triples),
                                                                 expr=TrueFilter))
                    else:
                        raise ExpressionNotCoveredException("FILTER NOT EXISTS clause has more than one "
                                                            "triple statement. This case is seemingly rare and "
                                                            "has not been covered yet. Therefore, FILTER NOT EXISTS "
                                                            "will not be translated into the OPTIONAL + !BOUND pattern."
                                                            "")

        try:
            algebra.traverse(q_algebra.algebra, replace_filter_not_exists)
        except ExpressionNotCoveredException as e:
            err = "Query will not be normalized because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        """
        #9
        Inverting the order of the triple statement (object predicate subject instead of subject predicate object) 
        using "^" yields the same result as if actually exchanging the subject and object within the triple statement.
        """
        pass
        # Solved with _resolve_paths (see #10)

        """
        #10
        Sequence paths can reduce the number of triples in the query statement and are commonly used. They can be
        resolved to 'normal' triple statements.
        """
        try:
            algebra.traverse(q_algebra.algebra, _resolve_paths)
        except ExpressionNotCoveredException as e:
            err = "Query will not be normalized because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        """
        #11
        Prefixes can be interchanged in the prefix section before the query and subsequently 
        in the query without changing the outcome
        """
        pass
        # Solved by translating the query into the query algebra. Prefixes get resolved.

        """
        #1
        A where clause will always be inserted.
        """
        pass
        # Solved by translating the query into the query algebra.

        """
        #2
        rdf:type" predicate can be replaced by "a".
        """
        pass
        # Solved by translating the query into the query algebra.

        """
        #3
        In case of an asterisk in the select-clause, all variables will be projected. However, this does not mean that 
        a query with explicitly stated variables will be equivalent to a query that uses the asterisk as a shortcut 
        to select all variables. This is due to the importance of the variables' order in the select clause.
        """
        pass
        # Solved by translating the query into the query algebra.

        """
        #7
        Variable names have no effect on the query semantic.
        Measure: Variables in the query tree will be replaced by letters from the alphabet. Only up to 26 variables 
        are supported. 
        There is no need to sort the variables in the query tree as this is implicitly solved by the algebra. Variables
        are sorted by their bindings where the ones with the most bindings come first.
        """
        q_vars_mapped = {}

        def retrieve_pv_vars(node):
            if isinstance(node, CompValue) and node.get('PV') and type(node.get('PV')) == list:
                for idx, pv in enumerate(node.get('PV')):
                    if isinstance(pv, Variable):
                        q_vars_mapped.setdefault(pv, chr(len(q_vars_mapped) + 97))
                    else:
                        raise ExpressionNotCoveredException("There is a project variable that is actually not "
                                                            "a variables but {0}. This case has not been covered yet."
                                                            "It will not be considered for replacement with a "
                                                            "letter from the alphabet.".format(type(pv)))

        def retrieve_bgp_vars(node):
            if isinstance(node, CompValue) and node.name == 'BGP':
                for triple in node.triples:
                    if isinstance(triple[0], Variable):
                        q_vars_mapped.setdefault(triple[0], chr(len(q_vars_mapped) + 97))
                    if isinstance(triple[1], Variable):
                        q_vars_mapped.setdefault(triple[1], chr(len(q_vars_mapped) + 97))
                    if isinstance(triple[2], Variable):
                        q_vars_mapped.setdefault(triple[2], chr(len(q_vars_mapped) + 97))

        def retrieve_bind_vars(node):
            if isinstance(node, CompValue) and node.name == 'Extend':
                q_vars_mapped.setdefault(node.var, chr(len(q_vars_mapped) + 97))

        def replace_variable_names_with_letters(node):
            if isinstance(node, Variable) and not node.startswith("__agg_"):
                try:
                    var = Variable(q_vars_mapped.get(node))
                    return var
                except Exception:
                    raise ExpressionNotCoveredException("The variable {0} is not found in: BGP, Bind. and will "
                                                        "not be replaced with a letter. Check whether something "
                                                        "went wrong prior to this step or the variable is not part "
                                                        "of any triple statement.".format(node))

        algebra.traverse(q_algebra.algebra, retrieve_bind_vars)
        algebra.traverse(q_algebra.algebra, retrieve_pv_vars)
        algebra.traverse(q_algebra.algebra, retrieve_bgp_vars)

        try:
            algebra.traverse(q_algebra.algebra, visitPost=replace_variable_names_with_letters)
        except ExpressionNotCoveredException as e:
            err = "Query will not be normalized because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        """
        #5
        Triple statements are first ordered by the number of their bindings. This is already done in the 
        query-to-algebra translation step. 
        Then, the query is ordered alphabetically by subject, predicate, object.
        """

        def reorder_triples(node):
            if isinstance(node, CompValue) and node.name == 'BGP':
                ordered_triples = {}
                try:
                    for t in node.triples:
                        norm_triple = ""

                        if isinstance(t[0], Variable):
                            try:
                                s = Variable(q_vars_mapped.get(t[0]))
                            except TypeError:
                                s = "does_not_exist"
                                logging.warning("Variable does not exist in q_vars_mapped.")
                        else:
                            s = t[0]

                        if isinstance(t[1], Variable):
                            p = Variable(q_vars_mapped.get(t[1]))
                        elif isinstance(t[1], Path):
                            logging.warning("A not resolved path was found: {0}. The whole triple statement will not "
                                            "be considered for re-ordering the triple statements.".format(t[1]))
                            continue
                        else:
                            p = t[1]

                        if isinstance(t[2], Variable):
                            o = Variable(q_vars_mapped.get(t[2]))
                        else:
                            o = t[2]
                        norm_triple += s + "_" + p + "_" + o
                        ordered_triples[(s, p, o)] = norm_triple
                        ordered_triples = {k: var for k, var in sorted(ordered_triples.items(),
                                                                       key=lambda item: item[1])}
                except Exception:
                    raise ExpressionNotCoveredException("There might be uncovered expressions, "
                                                        "identifiers or paths. Triple statements will not "
                                                        "be re-ordered.")

                node.triples.clear()
                node.triples.extend(list(ordered_triples.keys()))
            elif isinstance(node, CompValue) and node.name == 'TriplesBlock':
                raise ExpressionNotCoveredException("TriplesBlock has not been covered yet. "
                                                    "Triples within a TripleBlock Node will not be ordered. ")

        try:
            algebra.traverse(q_algebra.algebra, reorder_triples)
        except ExpressionNotCoveredException as e:
            err = "Query will not be normalized because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        return q_algebra

    def timestamp_query(self, query: str = None, citation_timestamp: datetime = None) -> str:
        """
        R7 - Query timestamping
        Binds a q_handler timestamp to the variable ?TimeOfExecution and wraps it around the query. Also extends
        the query with a code snippet that ensures that a snapshot of the data as of q_handler
        time gets returned when the query is executed. Optionally, but recommended, the order by clause
        is attached to the query to ensure a unique sort of the data.

        :param query:
        :param citation_timestamp:
        Use this parameter only for presentation purpose as the code for color encoding will make the SPARQL
        query erroneous!
        :return: A query string extended with the given timestamp
        """

        if query is None:
            if self.query is not None and self.sparql_prefixes is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                raise InputMissing("Query could not be normalized because the query string is not set.")
        else:
            prefixes, query = split_prefixes_query(query)
        query_vers = prefixes + "\n" + query

        if citation_timestamp is not None:
            timestamp = versioning_timestamp_format(citation_timestamp)
        else:
            timestamp = self.execution_timestamp

        bgp_triples = {}

        def inject_versioning_extensions(node):
            if isinstance(node, CompValue):
                if node.name == "BGP":
                    bgp_id = "BGP_" + str(len(bgp_triples))
                    bgp_triples[bgp_id] = node.triples.copy()
                    node.triples.append((rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_id)),
                                         rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_id)),
                                         rdflib.term.Literal('__{0}dummy_object__'.format(bgp_id))))
                elif node.name == "TriplesBlock":
                    raise ExpressionNotCoveredException("TriplesBlock has not been covered yet. "
                                                        "No versioning extensions will be injected.")

        query_tree = parser.parseQuery(query_vers)
        query_algebra = algebra.translateQuery(query_tree)
        try:
            algebra.traverse(query_algebra.algebra, visitPre=_resolve_paths)
            algebra.traverse(query_algebra.algebra, visitPre=inject_versioning_extensions)
        except ExpressionNotCoveredException as e:
            err = "Query will not be timestamped because of following error: {0}".format(e)
            raise ExpressionNotCoveredException(err)

        query_vers_out = _translate_algebra(query_algebra)
        for bgp_identifier, triples in bgp_triples.items():
            ver_block_template = \
                open(template_path("templates/query_utils/versioning_query_extensions.txt"), "r").read()

            ver_block = ""
            for i, triple in enumerate(triples):
                templ = ver_block_template
                triple_n3 = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
                ver_block += templ.format(triple_n3,
                                          "?triple_statement_{0}_valid_from".format(str(i)),
                                          "?triple_statement_{0}_valid_until".format(str(i)),
                                          bgp_identifier)

            dummy_triple = rdflib.term.Literal('__{0}dummy_subject__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_predicate__'.format(bgp_identifier)).n3() + " "\
                           + rdflib.term.Literal('__{0}dummy_object__'.format(bgp_identifier)).n3() + "."
            ver_block += 'bind("{0}"^^xsd:dateTime as ?TimeOfExecution{1})'.format(timestamp, bgp_identifier)
            query_vers_out = query_vers_out.replace(dummy_triple, ver_block)

        query_vers_out = versioning_prefixes("") + "\n" + query_vers_out

        return query_vers_out

    def compute_checksum(self, string: str = None) -> str:
        """
        Computes the checksum based on the provided :string or on
        the normalized query algebra (self.normalized_query_algebra) if no :string is provided
        with the sha256 algorithm.
        :string: The string to normalize. If no string is provided the checksum will be computed based on
        the normalized query algebra.
        :return: the hash value of either string or normalized query algebra
        """

        checksum = hashlib.sha256()
        if string is None:
            if self.normal_query is not None:
                checksum.update(str.encode(self.normal_query))
                return checksum.hexdigest()
            elif self.normal_query_algebra is not None:
                checksum.update(str.encode(self.normal_query_algebra.algebra))
                return checksum.hexdigest()
            else:
                raise InputMissing("Checksum could not be computed because the normalized "
                                   "query or query algebra is missing.")
        else:
            checksum.update(str.encode(string))
        return checksum.hexdigest()

    def generate_query_pid(self, query_checksum: str = None, citation_timestamp: datetime = None):
        """
        R8 - Query PID (The handling and fulfillment of this recommendation is done in q_handler.mint_query_pid)
        Generates a query PID by concatenating the provided query checksum and the q_handler timestamp from the
        QueryData object.

        :return:
        """
        if query_checksum is None:
            query_checksum = self.checksum
        if citation_timestamp is None:
            citation_timestamp = self.execution_timestamp
        else:
            citation_timestamp = versioning_timestamp_format(citation_timestamp)

        if None not in (query_checksum, citation_timestamp):
            query_pid = query_checksum + citation_timestamp
        else:
            query_pid = "No query PID could be generated because of missing parameters."
        return query_pid


class RDFDataSetUtils:

    def __init__(self, dataset: pd.DataFrame = None, description: str = None, unique_sort_index: tuple = None):
        """

        :param dataset:
        :param description:
        :param unique_sort_index:
        """

        self.dataset = dataset
        self.description = description
        self.sort_order = unique_sort_index
        if dataset is not None:
            self.checksum = self.compute_checksum()
        else:
            self.checksum = None

    def describe(self, dataset: pd.DataFrame = None, query: str = None):
        """
        Generates a description from the dataset. If a description is provided, the description will be returned.

        :return:
        """
        if self.dataset is None and dataset is None:
            raise InputMissing("No dataset was provided. Either use self.dataset or this function's parameter to "
                               "pass a dataset")
        if dataset is None:
            dataset = self.dataset.copy()

        # TODO: Use a natural language processor for RDF data to generate a description
        #  from the dataset and query
        if dataset.empty:
            return "This is an empty dataset. We cannot infer any description from it."
        dataset_description = dataset.describe()
        desc = ""
        if isinstance(dataset_description, pd.DataFrame):
            dataset_description = dataset_description.stack()
            dataset_description = dataset_description.sort_index()

            cnt_freq = pd.concat([dataset_description['count'], dataset_description['freq']],
                                 axis=1, join="inner")
            cnt_freq.rename(columns={0: 'count', 1: 'freq'}, inplace=True)
            cols_most_freq = cnt_freq.query("count == freq").index
            if len(cols_most_freq) > 0:
                desc += "This dataset is about: "
                desc += ", ".join(str(dataset[col].iloc[0]) for col in cols_most_freq) + "\n"

            desc += "Each column has following number of non-empty values: \n"
            desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['count'].items()) + "\n"
            desc += "For each column following frequencies were observed: "
            desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['freq'].items()) + "\n"
            desc += "Each column has following max/top values: "
            desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['top'].items()) + "\n"
            desc += "For each column following unique values were observed (opposite of frequencies): "
            desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['unique'].items()) + "\n"
        return desc

    def compute_checksum(self, dataset: pd.DataFrame = None, column_order_dependent: bool = False) -> str:
        """
        R6 - Result set verification
        A column order dependent or independent computation of the dataset checksum.

        Algorithm:
        Computes a checksum for each cell of the dataset. Then, a sum is calculated for each row making the
        checksum computation independent of the column order. Last, the vector of row sums is hashed again
        and the mean is taken.

        :param dataset:
        :param column_order_dependent: Tells the algorithm whether to make the checksum computation dependent or
        independent of the column order.
        :return:
        """

        if self.dataset is None and dataset is None:
            raise InputMissing("No dataset was provided. Either use self.dataset or this function's parameter to "
                               "pass a dataset")
        if dataset is None:
            dataset = self.dataset.copy()

        if column_order_dependent:
            row_hashsum_series = hash_pandas_object(dataset)
        else:
            def cell_hash_value(cell):
                checksum = hashlib.sha256()
                # numpy data types need to be converted to native python data types before they can be encoded
                checksum.update(str.encode(str(cell)))
                return int(checksum.hexdigest(), 16)

            # TODO: take care of empty result sets
            hashed_dataframe = self.dataset.apply(np.vectorize(cell_hash_value))
            row_hashsum_series = hashed_dataframe.sum(axis=1)

        row_hashsum_series = row_hashsum_series.astype(str)
        hash_sum = row_hashsum_series.str.cat()

        checksum = hashlib.sha256()
        checksum.update(str.encode(hash_sum))
        checksum = checksum.hexdigest()
        return checksum

    def create_sort_index(self, dataset: pd.DataFrame = None) -> list:
        """
        Infers the primary key from a dataset.
        Two datasets with differently permuted columns but otherwise identical
        will yield a different order of key attributes if one or more composite keys are suggested.
        Two datasets with differently re-mapped column names that would result into a different order when sorted
        will not affect the composition of the keys and therefore also not affect the sorting.
        A reduction of suggested composite keys might be made if there are two or more suggested composite keys. For
        each composite key the following will be done: 1. Count the number of distinct values for each key attribute
        (column); 2. Sum up all the counted distinct values.
        The composite keys with the maximum sum of distinct key attribute values will be returned.

        :return: A list of suggested indexes to use for sorting the dataset.
        """
        if self.dataset is None and dataset is None:
            raise InputMissing("No dataset was provided. Either use self.dataset or this function's parameter to "
                               "pass a dataset")
        if dataset is None:
            dataset = self.dataset.copy()

        # TODO: Think about whether the order of columns should yield a different permutation of key attributes
        #  within composite keys, thus, meaning a different sorting or not.

        def combination_util(combos: list, arr, n, tuple_size, data, index=0, i=0):
            """
            Returns a dictionary with combinations of size tuple_size.

            :param combos: list to be populated with the output combinations from this algorithm
            :param arr: Input Array
            :param n: Size of input array
            :param tuple_size: size of a combination to be printed
            :param index: Current index in data[]
            :param data: temporary list to store current combination
            :param i: index of current element in arr[]
            :return:
            """

            # Current combination is ready, print it
            if index == tuple_size:
                combo = []
                for j in range(tuple_size):
                    combo.append(data[j])
                combos.append(combo)
                return

            # When no more elements are there to put in data[]
            if i >= n:
                return

            # current is included, put next at next location
            data[index] = arr[i]
            combination_util(combos, arr, n, tuple_size, data, index + 1, i + 1)

            # current is excluded, replace it with next (Note that i+1 is passed, but index is not changed)
            combination_util(combos, arr, n, tuple_size, data, index, i + 1)

        sufficient_tuple_size = False
        df_key_finder = dataset.copy()
        df_key_finder.drop_duplicates(inplace=True)
        cnt_columns = len(df_key_finder.columns)
        columns = df_key_finder.columns

        distinct_occurrences = {}
        for column in columns:
            distinct_occurrences[column] = len(df_key_finder[column].unique().flat)

        attribute_combos_min_tuple_size = {}
        cnt_index_attrs = 1
        while not sufficient_tuple_size:
            attribute_combos = []
            combination_util(combos=attribute_combos, arr=columns, n=cnt_columns,
                             tuple_size=cnt_index_attrs, data=[0] * cnt_index_attrs)
            attribute_combos_unique_flags = {}
            for combo in attribute_combos:
                attribute_combos_unique_flags[tuple(combo)] = df_key_finder.set_index(combo).index.is_unique

            if any(attribute_combos_unique_flags.values()):
                sufficient_tuple_size = True
                attribute_combos_min_tuple_size = attribute_combos_unique_flags
            cnt_index_attrs += 1

        dist_stacked_key_attr_values = {}
        for composition, is_potential_key in attribute_combos_min_tuple_size.items():
            if is_potential_key:
                cnt_dist_stacked_attr_values = 0
                for attribute in composition:
                    cnt_distinct_attribute_values = distinct_occurrences[attribute]
                    cnt_dist_stacked_attr_values += cnt_distinct_attribute_values
                dist_stacked_key_attr_values[composition] = cnt_dist_stacked_attr_values

        max_val = max(dist_stacked_key_attr_values.values())
        sort_indexes = [k for k, v in dist_stacked_key_attr_values.items() if v == max_val]

        return sort_indexes

    def sort(self, sort_index: tuple = None, dataset: pd.DataFrame = None) -> pd.DataFrame:
        """
        R5 - Stable Sorting
        Sorts the dataset by sort_index. If none is given, a sort_index will be derived with the create_sort_index
        function.

        :sort_index: A user defined sort order for the dataset. If nothing is provided, the sort index will be derived
        from the dataset. Sometimes, this can yield ambiguous multi-indexes in which case the first multi-index will
        be taken from the list of possible unique indexes.
        :return: A sorted dataset
        """
        if self.dataset is None and dataset is None:
            raise InputMissing("No dataset was provided. Either use self.dataset or this function's parameter to "
                               "pass a dataset")
        if dataset is None:
            dataset = self.dataset.copy()

        if sort_index is None:
            sort_index_used = self.create_sort_index()
            if len(sort_index_used) != 1:
                raise MultipleSortIndexesError(
                    "The create_sort_index algorithm yields more than one possible unique sort index. "
                    "It is undecidable which one to take for sorting. Please provide one unique sort index.")
            sort_index_used = list(sort_index_used[0])
        else:
            # The tuple is passed in a list, therefore it must be taken out first
            sort_index_used = list(sort_index)
            is_unique = dataset.set_index(sort_index_used).index.is_unique

            if not is_unique:
                raise NoUniqueSortIndexError("A non-unique sort index was given.")

        self.sort_order = sort_index_used
        sorted_df = dataset.set_index(sort_index_used).sort_index()
        sorted_df = sorted_df.reset_index()

        return sorted_df


class MetaData:

    def __init__(self, identifier: str = None, creator: str = None, title: str = None, publisher: str = None,
                 publication_year: str = None, resource_type: str = None,
                 other_citation_data: dict = None, result_set_description: str = None):
        """
        Initialize the mandatory fields from DataCite's metadata model version 4.3 and additional metadata
        that the user can provide.
        """
        # TODO: Data operator defines which metadata to store. The user is able to change the description
        #  and other q_handler data. Possible solution: change other_citation_data to kwargs* (?)

        # Recommended fields to be populated. They are mandatory in the DataCite's metadata model
        self.identifier = identifier
        self.creator = creator
        self.title = title
        self.publisher = publisher
        self.publication_year = publication_year
        self.resource_type = resource_type

        # Other user-defined provenance data and other metadata
        self.other_citation_data = other_citation_data
        self.result_set_description = result_set_description

        self.citation_snippet = None

    def to_json(self) -> str:
        meta_data = vars(self).copy()
        del meta_data['citation_snippet']
        meta_data_json = json.dumps(meta_data)

        return meta_data_json

    def set_metadata(self, meta_data_json: str):
        """
        Reads the q_handler metadata provided as a json strings and creates the CitationData object.
        :param meta_data_json:
        :return: the q_handler metadata, but without the q_handler snippet
        """

        meta_data_dict = json.loads(meta_data_json)

        self.identifier = meta_data_dict['identifier']
        self.creator = meta_data_dict['creator']
        self.title = meta_data_dict['title']
        self.publisher = meta_data_dict['publisher']
        self.publication_year = meta_data_dict['publication_year']
        self.resource_type = meta_data_dict['resource_type']
        self.result_set_description = meta_data_dict['result_set_description']
        self.other_citation_data = meta_data_dict['other_citation_data']


def generate_citation_snippet(**kwargs) -> str:
    """
    R10 - Automated q_handler text
    Generates the q_handler snippet out of DataCite's mandatory attributes. Thus, following key parameters must be
    provided in an arbitrary order. The order will be reflected in the q_handler snippet:
    * identifier (DOI of query_pid)
    * creator
    * title
    * publisher
    * publication_year
    * resource type

    :return:
    """
    # TODO: Let the order of data within the snippet be defined by the user
    #  also: the user should be able to define which attributes are to be used in the q_handler snippet
    # TODO: Also allow other attributes next to the DateCite ones
    mandatory_attributes = ['identifier', 'creator', 'title', 'publisher', 'publication_year', 'resource_type']
    snippet = ", ".join(v for k, v in kwargs.items() if k in mandatory_attributes)

    return snippet
