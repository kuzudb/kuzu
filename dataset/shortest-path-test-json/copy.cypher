load extension "extension/json/build/libjson.kuzu_extension";
COPY person FROM "dataset/shortest-path-test-json/vPerson.json" (format='array', sample_size=1, maximum_depth=10);
COPY knows FROM "dataset/shortest-path-test-json/eKnows.json";
