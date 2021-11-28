#pragma once

#include "src/loader/include/in_mem_structure/lists_utils.h"
#include "src/loader/include/label_description.h"
#include "src/loader/include/thread_pool.h"
#include "src/storage/include/graph.h"

using namespace graphflow::common;
using namespace graphflow::storage;

namespace graphflow {
namespace loader {

class InMemStructuresBuilder {

public:
    // Saves all structures to disk.
    virtual void saveToFile() = 0;

protected:
    InMemStructuresBuilder(ThreadPool& threadPool, const Graph& graph, string outputDirectory);

protected:
    shared_ptr<spdlog::logger> logger;
    ThreadPool& threadPool;
    const Graph& graph;
    const string outputDirectory;
};

// Base class for InMemAdjAndPropertyColumnsBuilder and InMemAdjAndPropertyListsBuilder.
class InMemStructuresBuilderForRels : public InMemStructuresBuilder {

    typedef vector<unique_ptr<listSizes_t>> directionLabelNumRels_t;

public:
    // Sorts data in String overflow pages.
    virtual void sortOverflowStrings() = 0;

    void populateNumRelsInfo(
        vector<vector<vector<uint64_t>>>& numRelsPerDirBoundLabelRelLabel, bool forColumns);

protected:
    InMemStructuresBuilderForRels(RelLabelDescription& description, ThreadPool& threadPool,
        const Graph& graph, string outputDirectory)
        : InMemStructuresBuilder(threadPool, graph, outputDirectory), description(description){};

protected:
    RelLabelDescription& description;

    // To count the number of rels per adjLists/adjColumn
    directionLabelNumRels_t directionLabelNumRels{2};
};

} // namespace loader
} // namespace graphflow
