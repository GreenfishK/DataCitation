def generate_citation_snippet(**kwargs) -> str:
    """
    R10 - Automated citation text
    Generates the citation snippet out of DataCite's mandatory attributes. Thus, following key parameters must be
    provided in an arbitrary order. The order will be reflected in the citation snippet:
    * identifier (DOI of query_pid)
    * creator
    * title
    * publisher
    * publication_year
    * resource type

    :return:
    """
    # TODO: Let the order of data within the snippet be defined by the user
    #  also: the user should be able to define which attributes are to be used in the citation snippet
    mandatory_attributes = ['identifier', 'creator', 'title', 'publisher', 'publication_year', 'resource_type']
    snippet = ", ".join(v for k, v in kwargs.items() if k in mandatory_attributes)

    return snippet


citation_snippet = generate_citation_snippet(creator='creator1', identifier='id1', nonexistingattribute='test')
print(citation_snippet)