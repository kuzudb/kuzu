#include "spdlog/spdlog.h"

#include "src/loader/include/adj_and_prop_columns_builder.h"

namespace graphflow {
namespace loader {

AdjAndPropertyStructuresBuilder::AdjAndPropertyStructuresBuilder(RelLabelDescription& description,
    ThreadPool& threadPool, const Graph& graph, const string& outputDirectory)
    : logger{spdlog::get("loader")}, description{description}, threadPool{threadPool}, graph{graph},
      outputDirectory{outputDirectory} {};

void AdjAndPropertyStructuresBuilder::populateNumRelsInfo(
    vector<vector<vector<uint64_t>>>& numRelsPerDirBoundLabelRelLabel, bool forColumns) {
    for (auto& dir : DIRS) {
        if (forColumns) {
            if (description.isSingleCardinalityPerDir[dir]) {
                for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                    numRelsPerDirBoundLabelRelLabel[dir][boundNodeLabel][description.label] =
                        (*dirLabelNumRels[dir])[boundNodeLabel].load();
                }
            }
        } else {
            if (!description.isSingleCardinalityPerDir[dir]) {
                for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                    numRelsPerDirBoundLabelRelLabel[dir][boundNodeLabel][description.label] =
                        (*dirLabelNumRels[dir])[boundNodeLabel].load();
                }
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
