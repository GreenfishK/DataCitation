from setuptools import setup, find_packages

setup(
    # Metadata
    name="rdf_data_citation",
    version="0.7.0",
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
    package_data={'rdf_data_citation': ['persistence/*.sql', 'persistence/*.db', 'templates/*.txt']},
    install_requires=['python>=3.8', 'rdflib>=5.0.0', 'sparqlwrapper>=1.8.5', 'sqlalchemy>=1.3.19',
                      'sqlite', 'numpy>=1.18', 'numpy-base>=1.18', 'pandas>=1.1.2', 'nested-lookup>=0.2.22',
                      'jsonschema >=3.2.0', 'urllib3']

)
