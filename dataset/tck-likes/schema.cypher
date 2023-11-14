CREATE NODE TABLE A(ID SERIAL, name STRING, PRIMARY KEY(ID));
CREATE NODE TABLE B(ID SERIAL, name STRING, PRIMARY KEY(ID));
CREATE NODE TABLE C(ID SERIAL, name STRING, PRIMARY KEY(ID));
CREATE NODE TABLE D(ID SERIAL, name STRING, PRIMARY KEY(ID));
CREATE NODE TABLE E(ID SERIAL, name STRING, PRIMARY KEY(ID));
CREATE REL TABLE GROUP LIKES(FROM A TO B, FROM B TO C, FROM C TO D, FROM D TO E, FROM B TO A);
CREATE (n0:A {name: 'n0'}), (n00:B {name: 'n00'}), (n01:B {name: 'n01'}), (n000:C {name: 'n000'}), (n001:C {name: 'n001'}), (n010:C {name: 'n010'}), (n011:C {name: 'n011'}), (n0000:D {name: 'n0000'}), (n0001:D {name: 'n0001'}), (n0010:D {name: 'n0010'}), (n0011:D {name: 'n0011'}), (n0100:D {name: 'n0100'}), (n0101:D {name: 'n0101'}), (n0110:D {name: 'n0110'}), (n0111:D {name: 'n0111'}) CREATE (n0)-[:LIKES_A_B]->(n00), (n0)-[:LIKES_A_B]->(n01), (n00)-[:LIKES_B_C]->(n000), (n00)-[:LIKES_B_C]->(n001), (n01)-[:LIKES_B_C]->(n010), (n01)-[:LIKES_B_C]->(n011), (n000)-[:LIKES_C_D]->(n0000), (n000)-[:LIKES_C_D]->(n0001), (n001)-[:LIKES_C_D]->(n0010), (n001)-[:LIKES_C_D]->(n0011), (n010)-[:LIKES_C_D]->(n0100), (n010)-[:LIKES_C_D]->(n0101), (n011)-[:LIKES_C_D]->(n0110), (n011)-[:LIKES_C_D]->(n0111);
