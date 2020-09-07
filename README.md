# DataCitation
In order for the functions of this framework to work a triple store supporting SRAPQL* and RDF* needs to be setup, either locally or remotely. The only two parameters that need to be configured are the GET/read and POST/write endpoints. This is done by instantiating the class DataCitation and passing the two endpoint URIs to the constructor. These URIs, however, vary from solution to solution.

Examples for a locally set up "GraphDB free" database are found in the file "Playground". 
