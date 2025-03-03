# GDS Benchmarks

All algorithms are benchmarked on a slightly modified version of the [ego-Gplus dataset](https://snap.stanford.edu/data/ego-Gplus.html).
The dataset contains 107,614 nodes and 13,000,045 edges, with some edges updated from the original dataset to ensure 
multiple weakly connected components in the graph.

## Neo4j Setup

Results from Kuzu have been verified by comparing with Neo4j.

The `gds` and `apoc` plugins need to be installed. The docker images unfortunately download all the plugins specified in
`-e NEO4J_PLUGINS='...'` on every run. To avoid repeated downloads, specify `-e NEO4J_PLUGINS='...'` once to download the
plugins to the `/plugins` folder and then remove it from the subsequent runs.

```bash
$ cd /path/to/dataset

$ cat ./import/node_headers.csv                                                                
id:ID{id-type: int}

$ head -n3 ./import/nodes.csv                        
0
1
10

$ cat ./import/edge_headers.csv
:START_ID,:END_ID

$ head -n3 ./import/edges.csv    
36296,52511
17071,17922
20093,10963

# Import data, overwriting existing data
$ sudo docker run -it --rm --name neo4jimport --user="$(id -u):$(id -g)" \
  -v ./import:/import \
  -v ./data:/data \
  -v ./logs:/logs \
  -v ./config:/conf \
  -v ./plugins:/plugins \
  -e NEO4J_AUTH=none -e NEO4J_dbms_usage__report_enabled=false \
  -e NEO4J_server_memory_pagecache_size=4G -e NEO4J_server_memory_heap_max__size=4G \
  -e NEO4J_dbms_security_procedures_unrestricted=gds.\* \
  -e JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED \
  -e NEO4J_PLUGINS='["graph-data-science","apoc"]' \
  neo4j:latest bin/neo4j-admin database import full --overwrite-destination \
  --nodes=Node=/import/node_headers.csv,/import/nodes.csv \
  --relationships=Edge=/import/edge_headers.csv,/import/edges.csv

# Start Neo4j
$ sudo docker run -it --rm --name neo4j --user="$(id -u):$(id -g)" \
  -v ./import:/import \
  -v ./data:/data \
  -v ./logs:/logs \
  -v ./config:/conf \
  -v ./plugins:/plugins \
  -e NEO4J_AUTH=none -e NEO4J_dbms_usage__report_enabled=false \
  -e NEO4J_server_memory_pagecache_size=4G -e NEO4J_server_memory_heap_max__size=4G \
  -e NEO4J_dbms_security_procedures_unrestricted=gds.\* \
  -e JAVA_OPTS=--add-opens=java.base/java.nio=ALL-UNNAMED \
  -e NEO4J_apoc_export_file_enabled=true \
  neo4j:latest

# Run query
$ cat ./neo4j/wcc.cypher | sudo docker exec -i neo4j cypher-shell
...
componentId, idmin, count, idsum
0, 0, 107319, 5759958890
58114, 107287, 10, 1073652
57937, 107127, 9, 964728
57775, 106982, 8, 856074
57582, 106808, 7, 748231
57380, 106626, 6, 640205
57331, 106582, 5, 533039
57289, 106544, 4, 426229
57247, 106506, 3, 320595
57241, 106500, 2, 214085
```
