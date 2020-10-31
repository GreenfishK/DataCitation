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
    select ?document ?mention ?person ?personLabel ?value ?party where {

        ?document publishing:containsMention ?mention .
        ?mention publishing:hasInstance ?person .
        ?person pub:preferredLabel ?personLabel .
        ?person pub:memberOfPoliticalParty ?party .
        ?party pub:hasValue ?value .
        ?value pub:preferredLabel "Democratic Party"@en .
        filter(?personLabel = "Judy Chu"@en)
} 
"""

query_parser_test_2 = """
select * where {
    {
        ?document publishing:containsMention ?mention .
        ?mention publishing:hasInstance ?person .
        ?person pub:preferredLabel ?personLabel .
        ?person pub:memberOfPoliticalParty ?party .
        ?party pub:hasValue ?value .
        ?value pub:preferredLabel "Democratic Party"@en .
        filter(?personLabel = "Judy Chu"@en)

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

    }
}
"""

query_parser_test_3 = """

# get data that were valid at a certain timestamp - update
PREFIX citing: <http://ontology.ontotext.com/citing/>
PREFIX xsd: <http://www.w3.org/2001/XMLSchema#>
PREFIX pub: <http://ontology.ontotext.com/taxonomy/>
PREFIX owl: <http://www.w3.org/2002/07/owl#>
PREFIX rdf: <http://www.w3.org/1999/02/22-rdf-syntax-ns#>

select ?s ?p ?o ?valid_from ?valid_until  where {

    ?s ?p ?c.
    {select ?s ?p ?o where {
        # business logic - rows to update
        ?person a pub:Person .
        ?person pub:preferredLabel ?label .
        ?person pub:occupation ?occupation .
               {select ?occupation where
                {
                    ?occupation a pub:occupation.
                    ?occupation owl:ObjectProperty rdf:type.
                }    
            }
        filter(?label = "Fernando Alonso"@en)
        
        # Inputs to provide
        bind(?person as ?s)
        bind(pub:occupation as ?p)
        bind(?occupation as ?o) 
    }}
    
    bind("2020-10-03T14:11:21.941000+02:00"^^xsd:dateTime as ?TimeOfCiting)

}
"""

query_parser_test_4 = """

select ?document ?mention ?person ?personLabel ?value ?party where {
    ?document publishing:containsMention ?mention .
    ?mention publishing:hasInstance ?person .
    ?person pub:preferredLabel ?personLabel .
    ?value pub:preferredLabel "Democratic Party"@en .
    {
        select ?person ?party where {
            ?person pub:memberOfPoliticalParty ?party .
            ?party pub:hasValue ?value .
        }
    }
    filter(?personLabel = "Judy Chu"@en)
}
"""

query_parser_test_5 = """
select ?a ?b ?c ?d ?e ?f  {
    ?b publishing:hasInstance ?d .
    ?a publishing:containsMention ?b . 
    ?d pub:memberOfPoliticalParty ?c .
    ?d pub:preferredLabel ?e .
    ?c pub:hasValue ?f .
    ?f pub:preferredLabel "Democratic Party"@en .
    filter(?e = "Judy Chu"@en)
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
    
    bind(?mention as ?subjectToUpdate)
    bind(publishing:hasInstance as  ?predicateToUpdate)
    bind(?person as ?objectToUpdate) 

}


"""