import GraphDB.DataCitation as dc
from rdflib import URIRef, Literal

# Playground
citing = dc.DataVersioning('http://192.168.0.242:7200/repositories/DataCitation',
                           'http://192.168.0.242:7200/repositories/DataCitation/statements')

prefixes = {'citing': 'http://ontology.ontotext.com/citing/',
            'pub': 'http://ontology.ontotext.com/taxonomy/',
            'xsd': 'http://www.w3.org/2001/XMLSchema#'}

prefixes_2 = {'pub': 'http://ontology.ontotext.com/taxonomy/'}

triples_to_update_statement = """
    select * where {
            # business logic - rows to update
            ?person a pub:Person .
            ?person pub:preferredLabel ?label .
            ?person pub:occupation ?occupation .
            filter(?label = "Fernando Alonso"@en || ?label = "Sebastian Vettel"@en)

            # Inputs to provide
            bind(?person as ?subjectToUpdate)
            bind(pub:occupation as ?predicateToUpdate) 
            bind(?occupation as ?objectToUpdate) 
            
        }
        """


def test_read():
    citing.get_triples_to_update(triples_to_update_statement, prefixes)


def test_versioning():
    citing.reset_all_versions()
    citing.version_all_rows()


def test_update():
    triple = (URIRef("http://ontology.ontotext.com/resource/tsk9hdnas934"), URIRef("http://ontology.ontotext.com/taxonomy/occupation"), Literal("Cook"))
    citing.insert_triple(triple, prefixes_2)
    citing.delete_triples(triple, prefixes_2)


def test_update_with_versioning():
    new_value = 'Cashier'
    citing.update(triples_to_update_statement, new_value, prefixes)


def test_insert():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Merchant")
    citing.insert_triple((s,p,o), prefixes)


#test_read()
#test_update_with_versioning()
test_insert()


