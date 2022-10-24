CREATE NODE TABLE Person(name STRING, spouse STRING, PRIMARY KEY (name))
create REL TABLE Knows (FROM Person TO Person);
