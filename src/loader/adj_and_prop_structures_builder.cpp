#include "src/loader/include/adj_and_prop_structures_builder.h"

namespace graphflow {
namespace loader {

AdjAndPropertyStructuresBuilder::AdjAndPropertyStructuresBuilder(RelLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, string outputDirectory)
    : logger{LoggerUtils::getOrCreateSpdLogger("loader")}, description{description},
      threadPool{threadPool}, graph{graph}, outputDirectory{move(outputDirectory)} {}

void AdjAndPropertyStructuresBuilder::populateNumRelsInfo(
    vector<vector<vector<uint64_t>>>& numRelsPerDirBoundLabelRelLabel, bool forColumns) {
    for (auto& direction : DIRECTIONS) {
        if (forColumns) {
            if (description.isSingleMultiplicityPerDirection[direction]) {
                for (auto boundNodeLabel : description.nodeLabelsPerDirection[direction]) {
                    numRelsPerDirBoundLabelRelLabel[direction][boundNodeLabel][description.label] =
                        (*directionLabelNumRels[direction])[boundNodeLabel].load();
                }
            }
        } else {
            if (!description.isSingleMultiplicityPerDirection[direction]) {
                for (auto boundNodeLabel : description.nodeLabelsPerDirection[direction]) {
                    numRelsPerDirBoundLabelRelLabel[direction][boundNodeLabel][description.label] =
                        (*directionLabelNumRels[direction])[boundNodeLabel].load();
                }
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
