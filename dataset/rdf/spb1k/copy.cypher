COPY spb_resource_t FROM "dataset/rdf/spb1k/*.nq";
COPY spb_literal_t FROM glob("dataset/rdf/spb1k/*.nq");
COPY spb_resource_triples_t FROM glob("dataset/rdf/spb1k/*.nq");
COPY spb_literal_triples_t FROM glob("dataset/rdf/spb1k/*.nq");
