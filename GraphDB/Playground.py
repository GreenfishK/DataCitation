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
        PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
        PREFIX citing: <http://ontology.ontotext.com/citing/>
        PREFIX xsd: <http://www.w3.org/2001/XMLSchema>

        select ?subjectToUpdate ?predicateToUpdate ?objectToUpdate
        where {
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
new_value = "Cook"

########################################################################################################################
# testing read query
citing.get_triples_to_update(triples_to_update_statement)
#triple_to_remove = (URIRef(u'http://ontology.ontotext.com/resource/tsk9hdnas934'), URIRef(u'http://ontology.ontotext.com/taxonomy/occupation'), Literal('Cook'))

# testing add and delete
#triple = (URIRef("http://ontology.ontotext.com/resource/tsk9hdnas934"), URIRef("http://ontology.ontotext.com/taxonomy/occupation"), Literal("Cook"))
#citing.insert_triple(triple, prefixes_2)
#citing.delete_triples(triple, prefixes_2)

'''citing.insert_triple(triple=['<http://ontology.ontotext.com/resource/tsk9hdnas934>',
                             'pub:occupation',
                             'Car Mechanic'],
                     initNs={'citing': 'http://ontology.ontotext.com/citing/',
                             'pub': 'http://ontology.ontotext.com/taxonomy/',
                             'xsd': 'http://www.w3.org/2001/XMLSchema#'})
'''

# testing versioning
citing.reset_all_versions()
citing.version_all_rows()




