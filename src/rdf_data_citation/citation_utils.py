import copy
import logging

import rdflib.plugins.sparql.parser

from src.rdf_data_citation._helper import template_path, citation_timestamp_format
from src.rdf_data_citation.prefixes import split_prefixes_query, citation_prefixes
from src.rdf_data_citation.exceptions import NoVersioningMode, MultipleAliasesInBindError, NoDataSetError,\
    MultipleSortIndexesError, NoUniqueSortIndexError, NoQueryString, ExpressionNotCoveredException
from rdflib.plugins.sparql.parserutils import CompValue, Expr
from rdflib.term import Variable, Identifier
from rdflib.paths import SequencePath
import rdflib.plugins.sparql.parser as parser
import rdflib.plugins.sparql.algebra as algebra
from nested_lookup import nested_lookup
import pandas as pd
import hashlib
import datetime
from pandas.util import hash_pandas_object
import json
import numpy as np
import re
from datetime import datetime, timedelta, timezone
import tzlocal


def _query_algebra(query: str, sparql_prefixes: str) -> rdflib.plugins.sparql.algebra.Query:
    """
    Takes query and prefixes as strings and generates the query tree.
    :param query:
    :param sparql_prefixes: A query tree
    :return:
    """

    if sparql_prefixes:
        parse_tree = parser.parseQuery(sparql_prefixes + " " + query)
    else:
        parse_tree = parser.parseQuery(query)
    query_algebra = algebra.translateQuery(parse_tree)
    return query_algebra


def _query_variables(query: str, prefixes: str, variable_set_type: str = 'bgp') -> list:
    """
    The query must be a valid query including prefixes.
    The query algebra is searched for "PV". There can be more than one PV-Nodes containing the select-clause
    variables within a list. However, each of these lists enumerates the same variables only the first list
    will be selected and returned.

    :param variable_set_type: can be 'all', 'select' or 'order_by'
    :return: a list of variables used in the query
    """

    query_algebra = _query_algebra(query, prefixes)
    variables = []

    if variable_set_type == 'bgp':
        bgp_triples = {}

        def retrieve_bgp_variables(node):
            if isinstance(node, CompValue) and node.name == 'BGP':
                bgp_triples[node.name+str(len(bgp_triples))] = node.get('triples')
                return node
            else:
                return None
        algebra.traverse(query_algebra.algebra, retrieve_bgp_variables)

        for bgp_index, triple_list in bgp_triples.items():
            for triple in triple_list:
                if isinstance(triple[0], Variable):
                    variables.append(triple[0])
                if isinstance(triple[1], Variable):
                    variables.append(triple[1])
                if isinstance(triple[2], Variable):
                    variables.append(triple[2])
        variables = list(dict.fromkeys(variables))

    elif variable_set_type == 'select':
        def retrieve_select_variables(node):
            if isinstance(node, CompValue) and node.name == 'SelectQuery':
                variables.extend(node.get('PV'))
                return node.get('PV')
            else:
                del node
        algebra.traverse(query_algebra.algebra, retrieve_select_variables)

    elif variable_set_type == 'order_by':
        def retrieve_order_by_variables(node):
            if isinstance(node, CompValue) and node.name == 'OrderBy':
                order_by_conditions = node.get('expr')
                order_by_variables = []
                for order_condition in order_by_conditions:
                    variable = order_condition['expr']
                    order_by_variables.append(variable)
                    variables.append(variable)
                return order_by_variables
            else:
                del node

        # Traverses the query tree and extracts the variables into a list 'variables' that is defined above.
        algebra.traverse(query_algebra.algebra, retrieve_order_by_variables)

    return variables


def _to_sparql_query_text(query: str = None):
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
                replace("{BGP}", triples)
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
                    replace("{Filter}", "{" + node.p.name + "}HAVING({" + expr + "})")
                else:
                    replace("{Filter}", "{" + node.p.name + "}FILTER({" + expr + "})")
            elif node.name == "Union":
                replace("{Union}", "{" + node.p1.name + "}UNION{" + node.p2.name + "}")
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
                expr = "{" + node.p1.name + "}MINUS{{" + node.p2.name + "}}"
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
                replace("{OrderBy}", "{" + node.p.name + "}")
                replace("{OrderConditions}", " ".join(order_conditions))
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
                replace("{Project}", " ".join(project_variables) + "{{" + node.p.name + "}}" + order_by_pattern)
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
                    replace("{ToMultiSet}", "{SELECT " + "{" + node.p.name + "}" + "}")

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
            elif node.name.endswith('SUBSTR'):
                expr = "SUBSTR(" + node.arg.n3() + ", " + node.start + ", " + node.length + ")"
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
            elif node.name.endswith('CONCAT'):
                expr = 'CONCAT({vars})'.format(vars=", ".join(elem.n3() for elem in node.arg))
                replace("{Builtin_CONCAT}", expr)
            elif node.name.endswith('Builtin_LANGMATCHES'):
                replace("{Builtin_LANGMATCHES}", "LANGMATCHES(" + convert_node_arg(node.arg1)
                        + ", " + convert_node_arg(node.arg2) + ")")
            elif node.name.endswith('REGEX'):
                expr = "REGEX(" + node.text.n3() + ", " + node.pattern.n3() + ")"
                replace("{Builtin_REGEX}", expr)
            elif node.name.endswith('REPLACE'):
                replace("{Builtin_REPLACE}", "REPLACE(" + convert_node_arg(node.arg)
                        + ", " + convert_node_arg(node.pattern) + ", " + convert_node_arg(node.replacement) + ")")

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

    return query_algebra


class QueryUtils:

    def __init__(self, query: str = None, citation_timestamp: datetime = None):
        """
        Initializes the QueryData object and presets most of the variables by calling functions from this class.

        :param query: The SPARQL select statement that is used to retrieve the data set for citing.
        :param citation_timestamp: The timestamp as of citation.
        """

        if query is not None:
            self.sparql_prefixes, self.query = split_prefixes_query(query)
            self.bgp_variables = _query_variables(self.query, self.sparql_prefixes, 'bgp')
            self.select_variables = _query_variables(self.query, self.sparql_prefixes, 'select')
            self.order_by_variables = _query_variables(self.query, self.sparql_prefixes, 'order_by')
            self.normalized_query_algebra = str(self.normalize_query_tree().algebra)
            self.checksum = self.compute_checksum()

            if citation_timestamp is not None:
                self.citation_timestamp = citation_timestamp_format(citation_timestamp)  # -> str
            else:
                current_datetime = datetime.now()
                timezone_delta = tzlocal.get_localzone().dst(current_datetime).seconds
                execution_datetime = datetime.now(timezone(timedelta(seconds=timezone_delta)))
                execution_timestamp = citation_timestamp_format(execution_datetime)
                self.citation_timestamp = execution_timestamp
            self.timestamped_query = self.timestamp_query()
            self.pid = self.generate_query_pid()
        else:
            self.query = None
            self.bgp_variables = None
            self.normalized_query_algebra = None
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
                raise NoQueryString("Query could not be normalized because the query string was not set.")
        else:
            prefixes, query = split_prefixes_query(query)

        # Common SPARQL regex patterns
        re_variable = "[?][a-zA-Z0-9-_]*"
        re_prefix_ref = "[a-zA-Z0-9-_]*[:][a-zA-Z0-9-_]*"
        re_full_ref = "<[^\\s]>"
        re_value = '["].*["]'
        re_ref = re_full_ref + "|" + re_prefix_ref
        re_subject = re_variable + "|" + re_prefix_ref + "|" + re_full_ref
        re_predicate = re_variable + "|" + re_prefix_ref + "|" + re_full_ref
        re_object = re_variable + "|" + re_prefix_ref + "|" + re_full_ref + "|" + re_value

        """
        #6.1
        Aliases via BIND keyword just rename variables but the query result stays the same.
        
        """

        for var in self.bgp_variables:
            pattern = "bind\\s*[(]\\s*[?]{0}\\s*as\\s*{1}\\s*[)]".format(var, re_variable)
            binds = re.findall(pattern, query, re.MULTILINE)
            for bind in binds:
                pattern_alias_variable = "(?<=as)\\s*[?][a-zA-Z0-9-_]*"
                aliases = re.findall(pattern_alias_variable, bind)
                pattern_orig_var_name = "{0}\\s*(?=as)".format(re_variable)
                orig_var_names = re.findall(pattern_orig_var_name, bind)
                if len(aliases) == 1 and len(orig_var_names) == 1:
                    orig_var_name = orig_var_names[0].rstrip()
                    alias = aliases[0].lstrip()
                    query = query.replace(bind, "")
                    replace_pattern = "\{0}(\\s+|\\s*(?=\?))".format(alias)
                    query = re.sub(replace_pattern, orig_var_name, query)
                else:
                    raise MultipleAliasesInBindError("Check your bind statement in your "
                                                     "query and whether it has more than one alias.")

        """
        #6.2
        Aliases that are assigned in the select clause are just another means to #6.1 (using bind to
        assign aliases). 
        """
        pattern = "(?<=select)(.*)(\\s*[(](\\s*{variable})\\s*as\\s*{variable}\\s*[)])+".format(variable=re_variable)
        while re.findall(pattern, query, re.MULTILINE):
            query = re.sub(pattern, r"\1 \3", query)

        """
        #8 
        "filter not exists {triple}" expressions will be converted into the "filter + !bound" expression.
        """
        pattern = "(filter\\s*not\\s*exists\\s*)([{]" \
                  "(?:\\s*(?:" + re_subject + "))" \
                  "(?:\\s*(?:" + re_predicate + "))" \
                  "(\\s*(?:" + re_object + "))[}])"
        while re.findall(pattern, query, re.MULTILINE):
            query = re.sub(pattern, r"OPTIONAL \2 . filter(!bound(\3))", query)

        """
        #9
        Inverting the order of the triple statement (object predicate subject instead of subject predicate object) 
        using "^" yields the same result as if actually exchanging the subject and object within the triple statement.
        """
        pattern = "(\\s*(?:" + re_subject + "))" \
                  "(?:\\s*\^(" + re_ref + "))" \
                  "(\\s*(?:" + re_object + "))"
        while re.findall(pattern, query, re.MULTILINE):
            query = re.sub(pattern, r"\3 \2 \1", query)

        """
        #10
        Sequence paths can reduce the number of triplets in the query statement and are commonly used. They can be
        resolved to 'normal' triple statements.
        """
        pattern = "\\s*(" + re_subject + ")" \
                  "\\s*(" + re_predicate + ")" \
                  "\\s*[/]"
        dummy_cnt = 1
        while re.findall(pattern, query, re.MULTILINE):
            query = re.sub(pattern, r"\1 \2 ?dummy{0} . ?dummy{0} ".format(str(dummy_cnt)), query)
            dummy_cnt += 1
        q_algebra = _query_algebra(query, prefixes)

        """
        #11
        Prefixes can be interchanged in the prefix section before the query and subsequently 
        in the query without changing the outcome
        """
        pass
        # Implicitly solved by query algebra. Prefixes get resolved

        """
        #3
        In case of an asterisk in the select-clause, all variables will be explicitly mentioned 
        and ordered alphabetically.
        
        """
        pass
        # Implicitly solved by query algebra

        """
        #1
        A where clause will always be inserted
        """
        pass
        # Implicitly solved by query algebra

        """
        #5
        Triple statements will be ordered alphabetically by subject, predicate, object.
        The triple order is already unambiguous in the query algebra. The only requirement is
        that the variables in the select clause are in an unambiguous order. E.g. ?a ?b ?c ...
        """
        pass
        # Implicitly solved by query algebra

        """
        #7
        Variables in the query tree will be replaced by letters from the alphabet. For each variable 
        a letter from the alphabet will be assigned starting with 'a' and continuing in alphabetic order. 
        Two letters will be used  and combinations of letters will be assigned should there be more than 26 variables. 
        There is no need to sort the variables in the query tree as this is implicitly solved by the algebra. Variables
        are sorted by their bindings where the ones with the most bindings come first.
        """

        # Replace variables with letters
        int_norm_var = 'a'
        variables_mapping = {}
        extended_variables = _query_variables(query, prefixes, 'bgp')

        for i, v in enumerate(extended_variables):
            next_norm_var = chr(ord(int_norm_var) + i)
            variables_mapping.update({v: next_norm_var})

        # Replace rdflib.term.Variable('var') with var
        def norm_vars_1(var):
            if isinstance(var, Variable):
                return variables_mapping.get(var)
            else:
                return None

        algebra.traverse(q_algebra.algebra, norm_vars_1)

        # identify _var sets and sort the variables inside
        def norm_vars_2(x):
            if isinstance(x, set):
                normalized_variables = [variables_mapping[a] for a in x if not a.n3().startswith('?__agg')]
                return sorted(normalized_variables)
            else:
                return None

        algebra.traverse(q_algebra.algebra, norm_vars_2)

        # Sort variables (in select statement)
        def sort_list_of_string(list_node):
            if isinstance(list_node, list) and all(isinstance(list_item, str) for list_item in list_node):
                return sorted(list_node)
            else:
                return None

        algebra.traverse(q_algebra.algebra, visitPre=sort_list_of_string)

        return q_algebra

    def timestamp_query(self, query: str = None, citation_timestamp: datetime = None,
                        sort_variables: tuple = None, colored: bool = False):
        """
        R7 - Query timestamping
        Binds a citation timestamp to the variable ?TimeOfCiting and wraps it around the query. Also extends
        the query with a code snippet that ensures that a snapshot of the data as of citation
        time gets returned when the query is executed. Optionally, but recommended, the order by clause
        is attached to the query to ensure a unique sort of the data.

        :param query:
        :param citation_timestamp:
        :param sort_variables:
        :param colored: f colored is true, the injected strings within the statement template will be colored.
        Use this parameter only for presentation purpose as the code for color encoding will make the SPARQL
        query erroneous!
        :return: A query string extended with the given timestamp
        """

        if colored:
            red = ("\x1b[31m", "\x1b[0m")
            green = ("\x1b[32m", "\x1b[0m")
            blue = ("\x1b[34m", "\x1b[0m")
            magenta = ("\x1b[35m", "\x1b[0m")
            cyan = ("\x1b[36m", "\x1b[0m")
        else:
            red = ("", "")
            green = ("", "")
            blue = ("", "")
            magenta = ("", "")
            cyan = ("", "")

        template = open(template_path("templates/query_utils/query_wrapper.txt"), "r").read()

        if query is None:
            if self.query is not None:
                prefixes = self.sparql_prefixes
                query = self.query
            else:
                return "Query could not be normalized because the query string was not set."
        else:
            prefixes, query = split_prefixes_query(query)

        final_prefixes = citation_prefixes(prefixes)

        # Variables from the original query's select clause
        if self.bgp_variables is not None:
            select_variables = self.select_variables
        else:
            select_variables = _query_variables(query, final_prefixes, variable_set_type='select')
        variables_string = " ".join(v.n3() for v in select_variables)

        if citation_timestamp is not None:
            timestamp = citation_timestamp_format(citation_timestamp)
        else:
            timestamp = self.citation_timestamp

        # QueryData extensions for versioning_modes injection
        def query_triples(query, sparql_prefixes: str = None) -> list:
            """
            Takes a query and extracts the triple statements from it that are found in the query body.

            :return: A list of triple statements.
            """

            template = open(template_path("templates/query_utils/prefixes_query.txt"), "r").read()

            if sparql_prefixes:
                statement = template.format(sparql_prefixes, query)
            else:
                statement = template.format("", query)

            q_desc = parser.parseQuery(statement)
            q_algebra = algebra.translateQuery(q_desc).algebra
            triples = nested_lookup('triples', q_algebra)

            n3_triples = []
            for triple_list in triples:
                for triple in triple_list:
                    if isinstance(triple[1], SequencePath):
                        sequences = triple[1].args
                        for i, ref in enumerate(sequences, start=1):
                            if i == 1:
                                t = triple[0].n3() + " " + ref.n3() + " " + "?dummy{0}".format(str(i))
                            elif i == len(sequences):
                                t = "?dummy{0}".format(len(sequences) - 1) + " " + ref.n3() + " " + triple[2].n3()
                            else:
                                t = "?dummy{0}".format(str(i - 1)) + " " + ref.n3() + " " + "?dummy{0}".format(str(i))
                            n3_triples.append(t)
                    else:
                        t = triple[0].n3() + " " + triple[1].n3() + " " + triple[2].n3()
                        n3_triples.append(t)

            return sorted(n3_triples)

        triples = query_triples(query, final_prefixes)

        versioning_query_extensions_template = \
            open(template_path("templates/query_utils/versioning_query_extensions.txt"), "r").read()

        versioning_query_extensions = ""
        for i, triple in enumerate(triples):
            v = versioning_query_extensions_template
            versioning_query_extensions += v.format(triple,
                                                    "?triple_statement_{0}_valid_from".format(str(i)),
                                                    "?triple_statement_{0}_valid_until".format(str(i)))

        # Formatting and styling select statement
        normalized_query_formatted = query.replace('\n', '\n \t')

        decorated_query = template.format(final_prefixes,
                                          "{0}{1}{2}".format(red[0], variables_string, red[1]),
                                          "{0}{1}{2}".format(green[0], normalized_query_formatted, green[1]),
                                          "{0}{1}{2}".format(blue[0], timestamp, blue[1]),
                                          "{0}{1}{2}".format(magenta[0], versioning_query_extensions, magenta[1]),
                                          "{0}{1}{2}".format(cyan[0], "", cyan[1]))

        return decorated_query

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
            if self.normalized_query_algebra is not None:
                checksum.update(str.encode(self.normalized_query_algebra))
            else:
                return "Checksum could not be computed because the normalized query algebra is missing."
        else:
            checksum.update(str.encode(string))

        return checksum.hexdigest()

    def generate_query_pid(self, query_checksum: str = None, citation_timestamp: datetime = None):
        """
        R8 - Query PID (The handling and fulfillment of this recommendation is done in citation.cite)
        Generates a query PID by concatenating the provided query checksum and the citation timestamp from the
        QueryData object.

        :return:
        """
        if query_checksum is None:
            query_checksum = self.checksum
        if citation_timestamp is None:
            citation_timestamp = self.citation_timestamp
        else:
            citation_timestamp = citation_timestamp_format(citation_timestamp)

        if None not in (query_checksum, citation_timestamp):
            query_pid = query_checksum + citation_timestamp
        else:
            query_pid = "No query PID could be generated because of missing parameters."
        return query_pid


class RDFDataSetUtils:

    def __init__(self, dataset: pd.DataFrame = None, description: str = None,
                 unique_sort_index: tuple = None):
        """

        :param description:
        :param sort_order:
        """
        self.dataset = dataset
        self.description = description
        self.sort_order = unique_sort_index
        if dataset is not None:
            self.checksum = self.compute_checksum()
        else:
            self.checksum = None

    def describe(self, description: str = None):
        """
        Generates a description from the dataset. If a description is provided, the description will be returned.

        :return:
        """

        if description is None:
            # TODO: Use a natural language processor for RDF data to generate a description
            #  for the dataset which will be suggested.
            dataset = self.dataset.copy()
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
                    desc += ", ".join(dataset[col].iloc[0] for col in cols_most_freq) + "\n"

                desc += "Each column has following number of non-empty values: \n"
                desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['count'].items()) + "\n"
                desc += "For each column following frequencies were observed: "
                desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['freq'].items()) + "\n"
                desc += "Each column has following max/top values: "
                desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['top'].items()) + "\n"
                desc += "For each column following unique values were observed (opposite of frequencies): "
                desc += ", ".join(": ".join((str(k), str(v))) for k, v in dataset_description['unique'].items()) + "\n"
            return desc
        else:
            return description

    def compute_checksum(self, column_order_dependent: bool = False):
        """
        R6 - Result set verification
        A column order dependent or independent computation of the dataset checksum.

        Algorithm:
        Computes a checksum for each cell of the dataset. Then, a sum is calculated for each row making the
        checksum computation independent of the column order. Last, the vector of row sums is hashed again
        and the mean is taken.

        :param column_order_dependent: Tells the algorithm whether to make the checksum computation dependent or
        independent of the column order.
        :return:
        """

        if column_order_dependent:
            row_hashsum_series = hash_pandas_object(self.dataset)
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

    def create_sort_index(self) -> list:
        """
        Infers the primary key from a dataset.
        Two datasets with differently permuted columns but otherwise identical
        will yield a different order of key attributes if one or more composite keys are suggested.
        Two datasets with differently re-mapped column names that would result into a different order when sorted
        will not affect the composition of the keys and therefore also not affect the sorting.
        A reduction of suggested composite keys will be made if there are two or more suggested composite keys and
        the number of "distinct summed key attribute values" of each of these composite keys differ from each other.
        In fact, the composite keys with the minimum sum of distinct key attribute values will be returned.

        :return: A list of suggested indexes to use for sorting the dataset.
        """
        if self.dataset is None:
            raise NoDataSetError("No dataset was provided. This function needs self.dataset as an input")

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
        df_key_finder = self.dataset.copy()
        df_key_finder.drop_duplicates(inplace=True)
        cnt_columns = len(df_key_finder.columns)
        columns = df_key_finder.columns

        distinct_occurences = {}
        for column in columns:
            distinct_occurences[column] = len(df_key_finder[column].unique().flat)

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
                    cnt_distinct_attribute_values = distinct_occurences[attribute]
                    cnt_dist_stacked_attr_values += cnt_distinct_attribute_values
                dist_stacked_key_attr_values[composition] = cnt_dist_stacked_attr_values

        max_val = max(dist_stacked_key_attr_values.values())
        sort_indexes = [k for k, v in dist_stacked_key_attr_values.items() if v == max_val]

        return sort_indexes

    def sort(self, sort_index: tuple = None):
        """
        R5 - Stable Sorting
        Sorts by the index that is derived from create_sort_index. As this method can return more than one possible
        unique indexes, the first one will be taken.

        :sort_order: A user defined sort order for the dataset. If nothing is provided, the sort index will be derived
        from the dataset. Sometimes, this can yield ambiguous multi-indexes in which case the first multiindex will
        be taken from the list of possible unique indexes.
        :return:
        """
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
                 other_citation_data: dict = None):
        """
        Initialize the mandatory fields from DataCite's metadata model version 4.3
        """
        # TODO: Data operator defines which metadata to store. The user is able to change the description
        #  and other citation data. Possible solution: change other_citation_data to kwargs* (?)

        # Recommended fields to be populated. They are mandatory in the DataCite's metadata model
        self.identifier = identifier
        self.creator = creator
        self.title = title
        self.publisher = publisher
        self.publication_year = publication_year
        self.resource_type = resource_type

        # Other user-defined provenance data and other metadata
        self.other_citation_data = other_citation_data

        self.citation_snippet = None

    def to_json(self):
        meta_data = vars(self).copy()
        del meta_data['citation_snippet']
        meta_data_json = json.dumps(meta_data, indent=4)

        return meta_data_json

    def set_metadata(self, meta_data_json: str):
        """
        Reads the citation metadata provided as a json strings and creates the CitationData object.
        :param meta_data_json:
        :return: the citation metadata, but without the citation snippet
        """

        meta_data_dict = json.loads(meta_data_json)

        self.identifier = meta_data_dict['identifier']
        self.creator = meta_data_dict['creator']
        self.title = meta_data_dict['title']
        self.publisher = meta_data_dict['publisher']
        self.publication_year = meta_data_dict['publication_year']
        self.resource_type = meta_data_dict['resource_type']
        self.other_citation_data = meta_data_dict['other_citation_data']


def generate_citation_snippet(query_pid: str, citation_data: MetaData) -> str:
    """
    R10 - Automated citation text
    Generates the citation snippet out of DataCite's mandatory attributes within its metadata schema.
    :param query_pid:
    :param citation_data:
    :return:
    """
    # TODO: Let the order of data within the snippet be defined by the user
    #  also: the user should be able to define which attributes are to be used in the citation snippet
    snippet = "{0}, {1}, {2}, {3}, {4}, {5}, pid: {6} \n".format(citation_data.identifier,
                                                                 citation_data.creator,
                                                                 citation_data.title,
                                                                 citation_data.publisher,
                                                                 citation_data.publication_year,
                                                                 citation_data.resource_type,
                                                                 query_pid)
    return snippet
