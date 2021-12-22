#pragma once

#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/in_mem_structure/lists_utils.h"
#include "src/loader/include/label_description.h"
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
    InMemStructuresBuilder(
        TaskScheduler& taskScheduler, const Graph& graph, string outputDirectory);

protected:
    shared_ptr<spdlog::logger> logger;
    TaskScheduler& taskScheduler;
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
    InMemStructuresBuilderForRels(RelLabelDescription& description, TaskScheduler& taskScheduler,
        const Graph& graph, string outputDirectory)
        : InMemStructuresBuilder(taskScheduler, graph, outputDirectory), description(description){};

protected:
    RelLabelDescription& description;

    // To count the number of rels per adjLists/adjColumn
    directionLabelNumRels_t directionLabelNumRels{2};
};

} // namespace loader
} // namespace graphflow
