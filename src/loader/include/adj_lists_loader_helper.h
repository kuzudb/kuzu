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

typedef vector<vector<AdjListHeaders>> dirLabelAdjListHeaders_t;
typedef vector<vector<ListsMetadata>> dirLabelAdjListsMetadata_t;
typedef vector<vector<unique_ptr<InMemAdjLists>>> dirLabelAdjLists_t;
typedef vector<vector<vector<ListsMetadata>>> dirLabelPropertyIdxPropertyListsMetadata_t;
typedef vector<vector<vector<unique_ptr<InMemPropertyLists>>>> dirLabelPropertyIdxPropertyLists_t;

class AdjListsLoaderHelper {

public:
    AdjListsLoaderHelper(RelLabelDescription& description, const Graph& graph,
        const Catalog& catalog, const string outputDirectory, shared_ptr<spdlog::logger> logger);

    inline AdjListHeaders& getAdjListHeaders(Direction dir, label_t nodeLabel) {
        return (*dirLabelAdjListHeaders)[dir][nodeLabel];
    }

    inline ListsMetadata& getAdjListsMetadata(Direction dir, label_t nodeLabel) {
        return (*dirLabelAdjListsMetadata)[dir][nodeLabel];
    }

    inline ListsMetadata& getPropertyListsMetadata(
        Direction dir, label_t nodeLabel, uint32_t propertyIdx) {
        return (*dirLabelPropertyIdxPropertyListsMetadata)[dir][nodeLabel][propertyIdx];
    }

    inline InMemAdjLists& getAdjLists(Direction dir, label_t nodeLabel) {
        return *(*dirLabelAdjLists)[dir][nodeLabel];
    }

    inline InMemPropertyLists& getPropertyLists(
        Direction dir, label_t nodeLabel, uint32_t propertyIdx) {
        return *(*dirLabelPropertyIdxPropertyLists)[dir][nodeLabel][propertyIdx];
    }

    void buildInMemStructures();

    void saveToFile();

private:
    void buildInMemPropertyLists();

    void buildInMemAdjLists();

private:
    unique_ptr<dirLabelAdjListHeaders_t> dirLabelAdjListHeaders;

    unique_ptr<dirLabelAdjListsMetadata_t> dirLabelAdjListsMetadata;
    unique_ptr<dirLabelAdjLists_t> dirLabelAdjLists;

    unique_ptr<dirLabelPropertyIdxPropertyListsMetadata_t> dirLabelPropertyIdxPropertyListsMetadata;
    unique_ptr<dirLabelPropertyIdxPropertyLists_t> dirLabelPropertyIdxPropertyLists;

    shared_ptr<spdlog::logger> logger;
    RelLabelDescription& description;
    const Graph& graph;
    const Catalog& catalog;
    const string outputDirectory;
};

} // namespace loader
} // namespace graphflow
