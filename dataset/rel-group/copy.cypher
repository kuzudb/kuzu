COPY personA FROM "dataset/rel-group/node.csv" ;
COPY personB FROM "dataset/rel-group/node.csv" ;
COPY personC FROM "dataset/rel-group/node.csv" ;
COPY knows_personA_personB FROM "dataset/rel-group/edge.csv";
COPY knows_personA_personC FROM "dataset/rel-group/edge.csv";
COPY knows_personB_personC FROM "dataset/rel-group/edge.csv";
COPY likes_personA_personB FROM "dataset/rel-group/edge.csv";
COPY likes_personB_personA FROM "dataset/rel-group/edge.csv";
