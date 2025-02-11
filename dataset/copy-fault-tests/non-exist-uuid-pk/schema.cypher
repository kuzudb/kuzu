create node table person (fName UUID, PRIMARY KEY (fName));
create rel table like (FROM person TO person);
