from setuptools import setup, find_packages

setup(
    # Metadata
    name="rdf_data_citation",
    version="0.9.9.8",
    author="Filip Kovacevic",
    author_email="f.kovacevic@gmx.at",
    description="A package for versioning and citing RDF data.",
    license="GPL",
    keywords="example documentation tutorial",
    url="https://github.com/GreenfishK/DataCitation.git",
    long_description="See README.md file",
    classifiers=[
        "Programming Language :: Python :: 3",
        "License :: OSI Approved :: GNU General Public License (GPL)",
        "Operating System :: OS Independent",
    ],

    # Packaging
    include_package_data=True,
    packages=find_packages(
        where='src',
        include=['rdf_data_citation'],
    ),
    package_dir={"": "src"},
    package_data={'rdf_data_citation': ['persistence/*.sql', 'persistence/*.db', 'templates/query_utils/*.txt',
                                        'templates/rdf_star_store/*.txt',
                                        'templates/query_utils/versioning_modes/*.txt',
                                        'templates/rdf_star_store/test_connection/*.txt',
                                        'templates/rdf_star_store/versioning_modes/*.txt']},
    install_requires=['tzlocal>=2.1', 'pandas>=1.1.2', 'sparqlwrapper>=1.8.5', 'rdflib>=5.0.0', 'sqlalchemy>=1.3.19',
                      'numpy>=1.19.1', 'setuptools>=49.6.0']

)
