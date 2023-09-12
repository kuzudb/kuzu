create node table personA (ID INt64, fName StRING, PRIMARY KEY (ID));
create node table personB (ID INt64, fName StRING, PRIMARY KEY (ID));
create node table personC (ID INt64, fName StRING, PRIMARY KEY (ID));
create rel table group knows (FROM personA TO personB, FROM personA To personC, FROM personB TO personC, date DATE);
create rel table group likes (FROM personA TO personB, FROM personB To personA, date DATE);
