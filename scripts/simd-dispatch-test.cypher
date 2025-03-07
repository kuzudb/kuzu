CREATE NODE TABLE embeddings (id int64, vec FLOAT[8], PRIMARY KEY (id));
COPY embeddings FROM "dataset/embeddings/embeddings-8-1k.csv" (deLim=',');
CALL CREATE_HNSW_INDEX('e_hnsw_index', 'embeddings', 'vec', distFunc := 'l2');
CALL QUERY_HNSW_INDEX('e_hnsw_index', 'embeddings', CAST([0.1521,0.3021,0.5366,0.2774,0.5593,0.5589,0.1365,0.8557],'FLOAT[8]'), 3) RETURN nn.id ORDER BY _distance;
