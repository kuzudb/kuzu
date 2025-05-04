COPY `nodes` (`id`) FROM "dataset/snap/bitcoin-otc/nodes.csv" ;
COPY `edges` (`rating`,`time`) FROM "dataset/snap/bitcoin-otc/edges.csv" ;
