#include "src/loader/include/in_mem_structure/builder/in_mem_structures_builder.h"

namespace graphflow {
namespace loader {

InMemStructuresBuilder::InMemStructuresBuilder(
    TaskScheduler& taskScheduler, const Catalog& catalog, string outputDirectory)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")},
      taskScheduler{taskScheduler}, catalog{catalog}, outputDirectory{move(outputDirectory)} {}

uint64_t InMemStructuresBuilder::numProgressBarTasksForSavingPropertiesToDisk(
    const vector<PropertyDefinition>& properties) {
    uint64_t numTasks = properties.size();
    for (auto& property : properties) {
        if (STRING == property.dataType.typeID) {
            numTasks++;
        }
    }
    return numTasks;
}

} // namespace loader
} // namespace graphflow
