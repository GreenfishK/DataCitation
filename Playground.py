import DataCitationFramework.TSDataCitation as dc
import queries_for_testing as q
from rdflib import term
from datetime import datetime, timedelta, timezone
import re

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
    o = term.Literal("Football player")
    citing.insert_triple((s, p, o), q.prefixes)

def test_delete():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = term.Literal("Football player")
    citing._delete_triples((s, p, o), q.prefixes)

def test_outdate():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = term.Literal("Football player")
    citing.outdate_triples(q.triples_to_outdate_statement, q.prefixes)

def test_read_timestamp():
    vieTZObject = timezone(timedelta(hours=2))
    timestamp = datetime(2020, 9, 7, 12, 11, 21, 941000, vieTZObject)

    citing.get_data_at_timestamp(q.test_data_set_statement, timestamp, q.prefixes)

def test_extend_query_with_version_timestamp():
    vieTZObject = timezone(timedelta(hours=2))
    timestamp = datetime(2020, 10, 4, 12, 11, 21, 941000, vieTZObject)
    extended_query = citing.extend_query_with_timestamp(q.query_parser_test_2,timestamp, q.prefixes)
    print(extended_query)

#test_update_with_versioning()
#test_outdate()
#test_insert()
#test_delete()
#test_read_triples_to_update()
#test_read_timestamp()
#test_extend_query_with_version_timestamp()

# Outdate triple
#citing.outdate_triples(q.query_triples_to_outdate,w.prefixes)
#citing.outdate_triples(q.query_triples_to_outdate_2,q.prefixes)


s = [1,2,3]
print(s[0])