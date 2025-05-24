COPY personA FROM "dataset/rel-group/node.csv" ;
COPY personB FROM "dataset/rel-group/node.csv" ;
COPY personC FROM "dataset/rel-group/node.csv" ;
COPY knows FROM "dataset/rel-group/edge.csv" (from='personA', to='personB');
COPY knows FROM "dataset/rel-group/edge.csv" (from='personA', to='personC');
COPY knows FROM "dataset/rel-group/edge.csv" (from='personB', to='personC');
COPY likes FROM "dataset/rel-group/edge.csv" (from='personA', to='personB');
COPY likes FROM "dataset/rel-group/edge.csv" (from='personB', to='personA');
