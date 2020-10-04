prefixes = {'citing': 'http://ontology.ontotext.com/citing/',
            'pub': 'http://ontology.ontotext.com/taxonomy/',
            'xsd': 'http://www.w3.org/2001/XMLSchema#',
            'publishing': 'http://ontology.ontotext.com/publishing#'}

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

test_data_set_statement = """
select ?s ?p ?o where {
        # business logic - rows to update
        ?person a pub:Person .
        ?person pub:preferredLabel ?label .
        ?person pub:occupation ?occupation .
        filter(?label = "Fernando Alonso"@en)

        # Inputs to provide
        bind(?person as ?s)
        bind(pub:occupation as ?p)
        bind(?occupation as ?o) 
    }

"""

query_parser_test = """
PREFIX publishing: <http://ontology.ontotext.com/publishing#>
PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
PREFIX citing: <http://ontology.ontotext.com/citing/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

select * where {
    {
        ?document publishing:containsMention ?mention .
        ?mention publishing:hasInstance ?person .
        ?person pub:preferredLabel ?personLabel .
        ?person pub:memberOfPoliticalParty ?party .
        ?party pub:hasValue ?value .
        ?value pub:preferredLabel "Democratic Party"@en .
        filter(?personLabel = "Judy Chu"@en)
        
        bind(?document as ?s)
        bind (publishing:containsMention as  ?p )
        bind(?mention as ?o) 
        bind("2020-10-03T14:11:21.941000+02:00"^^xsd:dateTime as ?TimeOfCiting)

    }
    union
    {
        ?document publishing:containsMention ?mention .
        ?mention publishing:hasInstance ?person .
        ?person pub:preferredLabel ?personLabel .
        ?person pub:memberOfPoliticalParty ?party .
        ?party pub:hasValue ?value .
        ?value pub:preferredLabel "Democratic Party"@en .
        filter(?personLabel = "Marlon Brando"@en)

        bind(?document as ?s)
        bind (publishing:containsMention as  ?p )
        bind(?mention as ?o) 
        bind("2020-10-03T14:11:21.941000+02:00"^^xsd:dateTime as ?TimeOfCiting)

    }
}
"""

query_parser_test_2 = """
PREFIX publishing: <http://ontology.ontotext.com/publishing#>
PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
PREFIX citing: <http://ontology.ontotext.com/citing/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>

select * where {
    
        ?document publishing:containsMention ?mention .
        ?mention publishing:hasInstance ?person .
        ?person pub:preferredLabel ?personLabel .
        ?person pub:memberOfPoliticalParty ?party .
        ?party pub:hasValue ?value .
        ?value pub:preferredLabel "Democratic Party"@en .
        filter(?personLabel = "Judy Chu"@en)

        bind(?document as ?s)
        bind (publishing:containsMention as  ?p )
        bind(?mention as ?o) 
        bind("2020-10-03T14:11:21.941000+02:00"^^xsd:dateTime as ?TimeOfCiting)
        
   
}
"""

query_triples_to_outdate = """
select * WHERE {
    ?document publishing:containsMention ?mention .
    ?mention publishing:hasInstance ?person .
    ?person pub:preferredLabel ?personLabel .
    ?person pub:memberOfPoliticalParty ?party .
    ?party pub:hasValue ?value .
    ?value pub:preferredLabel "Democratic Party"@en .
    filter(?personLabel = "Judy Chu"@en)
    filter(?mention = <http://data.ontotext.com/publishing#Mention-dbaa4de4563be5f6b927c87e09f90461c09451296f4b52b1f80dcb6e941a5acd>)

    bind(?document as ?subjectToUpdate)
    bind(publishing:containsMention as  ?predicateToUpdate)
    bind(?mention as ?objectToUpdate) 

}


"""

query_triples_to_outdate_2 = """
select * WHERE {
    ?document publishing:containsMention ?mention .
    ?mention publishing:hasInstance ?person .
    ?person pub:preferredLabel ?personLabel .
    ?person pub:memberOfPoliticalParty ?party .
    ?party pub:hasValue ?value .
    ?value pub:preferredLabel "Democratic Party"@en .

    filter(?value = <http://ontology.ontotext.com/resource/tsk5a9unh5a8>)

    bind(?document as ?subjectToUpdate)
    bind(publishing:containsMention as  ?predicateToUpdate)
    bind(?mention as ?objectToUpdate) 

}


"""