#pragma once

#include "src/common/include/task_system/task_scheduler.h"
#include "src/loader/include/in_mem_structure/lists_utils.h"
#include "src/loader/include/label_description.h"
#include "src/loader/include/loader_progress_bar.h"
#include "src/storage/include/storage_manager.h"

namespace graphflow {
namespace loader {

class InMemStructuresBuilder {

public:
    // Saves all structures to disk.
    virtual void saveToFile(LoaderProgressBar* progressBar) = 0;

protected:
    InMemStructuresBuilder(
        TaskScheduler& taskScheduler, const Catalog& catalog, string outputDirectory);

    uint64_t numProgressBarTasksForSavingPropertiesToDisk(
        const vector<PropertyDefinition>& properties);

protected:
    shared_ptr<spdlog::logger> logger;
    TaskScheduler& taskScheduler;
    const Catalog& catalog;
    const string outputDirectory;
};

// Base class for InMemAdjAndPropertyColumnsBuilder and InMemAdjAndPropertyListsBuilder.
class InMemStructuresBuilderForRels : public InMemStructuresBuilder {

    typedef vector<unique_ptr<listSizes_t>> directionLabelNumRels_t;

public:
    // Sorts data in String overflow pages.
    virtual void sortOverflowStrings(LoaderProgressBar* progressBar) = 0;

    inline uint64_t getNumRelsForDirectionLabel(RelDirection relDirection, label_t labelId) {
        return directionLabelNumRels[relDirection]->operator[](labelId).load();
    }

protected:
    InMemStructuresBuilderForRels(RelLabelDescription& description, TaskScheduler& taskScheduler,
        const Catalog& catalog, const string& outputDirectory)
        : InMemStructuresBuilder(taskScheduler, catalog, outputDirectory),
          description(description){};

protected:
    RelLabelDescription& description;

    // To count the number of rels per adjLists/adjColumn
    directionLabelNumRels_t directionLabelNumRels{2};
};

} // namespace loader
} // namespace graphflow
