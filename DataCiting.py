import rdflib
from rdflib import ConjunctiveGraph
from rdflib.plugins.sparql import prepareQuery
from rdflib.plugins.sparql import update
from rdflib.plugins.stores import sparqlstore
import pprint

g = ConjunctiveGraph('Sleepycat')
g.open('./graphStore', create=False)


# Version all rows in the graph
version_all_rows = prepareQuery("""
# version all rows with latest version
PREFIX citing: <http://ontology.ontotext.com/citing/>
# initial versioning
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
insert 
{
    <<?s ?p ?o>> citing:valid_from ?currentTimestamp;
                 citing:valid_until "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime.        
}
where
{
   ?s ?p ?o .
   BIND(xsd:dateTime(NOW()) AS ?currentTimestamp).
}""")

g.query(version_all_rows)

# Does not work yet because rdflib's graph store does not support rdf* and sparql*
rows_to_update = prepareQuery(
    """
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
        bind("Cook" as ?newValue). #new Value
        
        <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_from ?valid_from .
        <<?subjectToUpdate ?predicateToUpdate ?objectToUpdate>> citing:valid_until ?valid_until .  
        BIND(xsd:dateTime(NOW()) AS ?newVersion). # multiple ?versions are retrieved leading to multiple updates. TODO: fix this
        filter(?valid_until = "9999-12-31T00:00:00.000+02:00"^^xsd:dateTime)
        filter(?newValue != ?objectToUpdate) # nothing should be changed if old and new value are the same  
    }
    """,
    initNs={'pub': 'http://ontology.ontotext.com/taxonomy/',
            'citing': 'http://ontology.ontotext.com/citing/',
            'xsd': 'http://www.w3.org/2001/XMLSchema#'}
)


for row in g.query(rows_to_update):
    print(row)

