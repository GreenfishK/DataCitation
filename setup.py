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
    packages=find_packages(
        where='src',
        include=['rdf_data_citation'],
    ),
    package_dir={"": "src"}
)
