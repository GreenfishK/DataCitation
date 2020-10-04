import GraphDB.DataCitation as dc
import queries_for_testing as q
from rdflib import URIRef, Literal
from datetime import datetime, timedelta, timezone
from rdflib.plugins.sparql.parser import parseQuery
import rdflib.plugins.sparql.algebra as algebra
import rdflib.plugins.sparql.parserutils as parserutils

# Playground
citing = dc.DataVersioning('http://192.168.0.242:7200/repositories/DataCitation', #GET
                           'http://192.168.0.242:7200/repositories/DataCitation/statements') #POST





def test_read_triples_to_update():
    new_value = 'Merchant'
    citing.get_triples_to_update(q.triples_to_update_statement, new_value, q.prefixes)


def test_versioning():
    citing.reset_all_versions()
    citing.version_all_rows()


def test_update_with_versioning():
    new_value = 'Cashier'
    citing.update(q.triples_to_update_statement, new_value, q.prefixes)


def test_insert():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing.insert_triple((s, p, o), q.prefixes)

def test_delete():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing._delete_triples((s, p, o), q.prefixes)

def test_outdate():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing.outdate_triples(q.triples_to_outdate_statement, q.prefixes)

def test_read_timestamp():
    vieTZObject = timezone(timedelta(hours=2))
    timestamp = datetime(2020, 9, 7, 12, 11, 21, 941000, vieTZObject)

    citing.get_data_at_timestamp(q.test_data_set_statement, timestamp, q.prefixes)


#test_update_with_versioning()
#test_outdate()
#test_insert()
#test_delete()
#test_read_triples_to_update()
#test_read_timestamp()

# Outdate triple
#citing.outdate_triples(query_triples_to_outdate,prefixes)
#citing.outdate_triples(query_triples_to_outdate_2,prefixes)

# Parse triple patterns
q_desc = parseQuery(q.query_parser_test)
q_algebra = algebra.translateQuery(q_desc).algebra
#pprintAlgebra(q_trans)
print(type(q_desc))


#print(q[1]['where']['part'])##
#for x in q[1]['where']['part'][0]['triples']:
#    print(x)

def get_val_from_path_1(d, p):
    sentinel = object()
    d = d.get(p[0], sentinel)
    if d == sentinel:
        return None
    if len(p) > 1:
        return get_val_from_path_1(d, p[1:])
    return d

get_val_from_path_1(q_algebra, 'triples')

parserutils.prettify_parsetree(q_desc)