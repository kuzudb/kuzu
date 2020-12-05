#pragma once

#include "spdlog/sinks/stdout_sinks.h"
#include "spdlog/spdlog.h"

#include "src/loader/include/in_mem_structures.h"
#include "src/loader/include/utils.h"
#include "src/storage/include/catalog.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

typedef vector<vector<unique_ptr<InMemPropertyColumn>>> labelPropertyIdxPropertyColumn_t;
typedef vector<vector<unique_ptr<InMemAdjRels>>> dirLabelAdjRels_t;

class AdjRelsLoaderHelper {

public:
    AdjRelsLoaderHelper(RelLabelDescription& description, const Graph& graph,
        const Catalog& catalog, const string outputDirectory, shared_ptr<spdlog::logger> logger);

    void saveToFile();

    inline InMemAdjRels& getAdjRels(Direction dir, label_t nodelabel) {
        return *(*dirLabelAdjRels)[dir][nodelabel];
    }

    inline InMemPropertyColumn& getPropertyColumn(label_t nodeLabel, uint32_t propertyIdx) {
        return *(*labelPropertyIdxPropertyColumn)[nodeLabel][propertyIdx];
    }

private:
    void buildInMemPropertyColumns(Direction dir);

    void buildInMemAdjRels();

private:
    unique_ptr<labelPropertyIdxPropertyColumn_t> labelPropertyIdxPropertyColumn;
    unique_ptr<dirLabelAdjRels_t> dirLabelAdjRels;

    shared_ptr<spdlog::logger> logger;
    RelLabelDescription& description;
    const Graph& graph;
    const Catalog& catalog;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
