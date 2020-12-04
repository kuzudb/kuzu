#include "src/loader/include/adj_rels_loader_helper.h"

#include "src/storage/include/stores/rels_store.h"

namespace graphflow {
namespace loader {

AdjRelsLoaderHelper::AdjRelsLoaderHelper(RelLabelDescription& description, ThreadPool& threadPool,
    const Graph& graph, const Catalog& catalog, const string outputDirectory,
    shared_ptr<spdlog::logger> logger)
    : logger{logger}, description{description},
      threadPool{threadPool}, graph{graph}, catalog{catalog}, outputDirectory{outputDirectory} {
    if (description.hasProperties()) {
        if (description.isSingleCardinalityPerDir[FWD]) {
            buildInMemPropertyColumns(FWD);
        } else if (description.isSingleCardinalityPerDir[BWD]) {
            buildInMemPropertyColumns(BWD);
        }
    }
    buildInMemAdjRels();
}

void AdjRelsLoaderHelper::buildInMemPropertyColumns(Direction dir) {
    logger->info("Creating InMemProperty Columns...");
    labelPropertyIdxPropertyColumn =
        make_unique<labelPropertyIdxPropertyColumn_t>(catalog.getNodeLabelsCount());
    for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
        (*labelPropertyIdxPropertyColumn)[nodeLabel].resize((*description.propertyMap).size());
        auto numElements = graph.getNumNodesForLabel(nodeLabel);
        for (auto i = 0u; i < (*description.propertyMap).size(); i++) {
            auto dataType = (*description.propertyMap)[i].dataType;
            auto fname = RelsStore::getRelPropertyColumnFname(
                outputDirectory, description.label, nodeLabel, (*description.propertyMap)[i].name);
            if (NODE != dataType && LABEL != dataType && STRING != dataType) {
                (*labelPropertyIdxPropertyColumn)[nodeLabel][i] =
                    make_unique<InMemPropertyColumn>(fname, numElements, getDataTypeSize(dataType));
            }
        }
    }
}

void AdjRelsLoaderHelper::buildInMemAdjRels() {
    logger->info("Creating InMemAdjRels...");
    dirLabelAdjRels = make_unique<dirLabelAdjRels_t>(2);
    for (auto& dir : DIRS) {
        if (description.isSingleCardinalityPerDir[dir]) {
            (*dirLabelAdjRels)[dir].resize(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                auto fname = RelsStore::getAdjRelsIndexFname(
                    outputDirectory, description.label, boundNodeLabel, dir);
                (*dirLabelAdjRels)[dir][boundNodeLabel] =
                    make_unique<InMemAdjRels>(fname, graph.getNumNodesForLabel(boundNodeLabel),
                        description.numBytesSchemePerDir[dir].first,
                        description.numBytesSchemePerDir[dir].second);
            }
        }
    }
}

void AdjRelsLoaderHelper::saveToFile() {
    for (auto& dir : DIRS) {
        if (description.isSingleCardinalityPerDir[dir]) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                threadPool.execute([&](InMemAdjRels* x) { x->saveToFile(); },
                    (*dirLabelAdjRels)[dir][nodeLabel].get());
            }
        }
    }
    if (labelPropertyIdxPropertyColumn) {
        for (auto& columns : *labelPropertyIdxPropertyColumn) {
            for (auto& column : columns) {
                if (column) {
                    threadPool.execute(
                        [&](InMemPropertyColumn* x) { x->saveToFile(); }, column.get());
                }
            }
        }
    }
    threadPool.wait();
}

} // namespace loader
} // namespace graphflow
