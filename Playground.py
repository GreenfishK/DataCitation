import GraphDB.DataCitation as dc
from rdflib import URIRef, Literal

# Playground
citing = dc.DataVersioning('http://192.168.0.242:7200/repositories/DataCitation', #GET
                           'http://192.168.0.242:7200/repositories/DataCitation/statements') #POST

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
            filter(?label = "Fernando Alonso"@en)
    
            # Inputs to provide
            bind(?person as ?subjectToUpdate)
            bind(pub:occupation as ?predicateToUpdate) 
            bind(?occupation as ?objectToUpdate) 
            
        }
"""

triples_to_outdate_statement = """
    select ?subjectToUpdate ?predicateToUpdate ?objectToUpdate where {
        # business logic - rows to update
        ?person a pub:Person .
        ?person pub:preferredLabel ?label .
        ?person pub:occupation ?occupation .
        filter(?label = "Fernando Alonso"@en)
        filter(?occupation = "Football player")
        
        # Inputs to provide
        bind(?person as ?subjectToUpdate)
        bind(pub:occupation as ?predicateToUpdate)
        bind(?occupation as ?objectToUpdate) 
    }
"""

data_to_cite_statement = """


"""

def test_read_triples_to_update():
    new_value = 'Merchant'
    citing.get_triples_to_update(triples_to_update_statement, new_value, prefixes)


def test_versioning():
    citing.reset_all_versions()
    citing.version_all_rows()


def test_update_with_versioning():
    new_value = 'Cashier'
    citing.update(triples_to_update_statement, new_value, prefixes)


def test_insert():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing.insert_triple((s, p, o), prefixes)

def test_delete():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing._delete_triples((s, p, o), prefixes)

def test_outdate():
    s = "<http://ontology.ontotext.com/resource/tsk9hdnas934>"
    p = "pub:occupation"
    o = Literal("Football player")
    citing.outdate_triples(triples_to_outdate_statement, prefixes)

def test_read_timestamp():
    citing.get_data_at_timestamp()
#test_update_with_versioning()
#test_outdate()
#test_insert()
#test_delete()
#test_read_triples_to_update()
test_read_timestamp()