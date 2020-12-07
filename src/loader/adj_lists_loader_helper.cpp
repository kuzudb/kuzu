#include "src/loader/include/adj_lists_loader_helper.h"

namespace graphflow {
namespace loader {

AdjListsLoaderHelper::AdjListsLoaderHelper(RelLabelDescription& description, const Graph& graph,
    const Catalog& catalog, const string outputDirectory, shared_ptr<spdlog::logger> logger)
    : logger{logger}, description{description}, graph{graph}, catalog{catalog},
      outputDirectory{outputDirectory} {
    logger->info("Creating AdjLists and PropertyLists Metadata...");
    dirLabelAdjListHeaders = make_unique<dirLabelAdjListHeaders_t>(2);
    dirLabelAdjListsMetadata = make_unique<dirLabelAdjListsMetadata_t>(2);
    dirLabelPropertyIdxPropertyListsMetadata =
        make_unique<dirLabelPropertyIdxPropertyListsMetadata_t>(2);
    for (auto& dir : DIRS) {
        (*dirLabelAdjListHeaders)[dir].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            (*dirLabelAdjListHeaders)[dir][nodeLabel].headers.resize(
                graph.getNumNodesForLabel(nodeLabel));
        }
        (*dirLabelAdjListsMetadata)[dir].resize(catalog.getNodeLabelsCount());
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            (*dirLabelPropertyIdxPropertyListsMetadata)[dir].resize(catalog.getNodeLabelsCount());
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                (*dirLabelPropertyIdxPropertyListsMetadata)[dir][nodeLabel].resize(
                    description.propertyMap->size());
            }
        }
    }
}

void AdjListsLoaderHelper::buildInMemStructures() {
    if (description.requirePropertyLists()) {
        buildInMemPropertyLists();
    }
    buildInMemAdjLists();
}

void AdjListsLoaderHelper::buildInMemAdjLists() {
    logger->info("Creating InMemAdjLists...");
    dirLabelAdjLists = make_unique<dirLabelAdjLists_t>(2);
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            (*dirLabelAdjLists)[dir].resize(catalog.getNodeLabelsCount());
            for (auto boundNodeLabel : description.nodeLabelsPerDir[dir]) {
                auto fname = RelsStore::getAdjListsIndexFname(
                    outputDirectory, description.label, boundNodeLabel, dir);
                (*dirLabelAdjLists)[dir][boundNodeLabel] = make_unique<InMemAdjLists>(fname,
                    getAdjListsMetadata(dir, boundNodeLabel).numPages,
                    description.numBytesSchemePerDir[dir].first,
                    description.numBytesSchemePerDir[dir].second);
            }
        }
    }
}

void AdjListsLoaderHelper::buildInMemPropertyLists() {
    logger->info("Creating InMemPropertyLists...");
    dirLabelPropertyIdxPropertyLists = make_unique<dirLabelPropertyIdxPropertyLists_t>(2);
    for (auto& dir : DIRS) {
        (*dirLabelPropertyIdxPropertyLists)[dir].resize(catalog.getNodeLabelsCount());
        for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
            (*dirLabelPropertyIdxPropertyLists)[dir][nodeLabel].resize(
                description.propertyMap->size());
            for (auto i = 0u; i < description.propertyMap->size(); i++) {
                auto& property = (*description.propertyMap)[i];
                if (NODE != property.dataType && LABEL != property.dataType &&
                    STRING != property.dataType) {
                    auto fname = RelsStore::getRelPropertyListsFname(
                        outputDirectory, description.label, nodeLabel, dir, property.name);
                    (*dirLabelPropertyIdxPropertyLists)[dir][nodeLabel][i] =
                        make_unique<InMemPropertyLists>(fname,
                            getPropertyListsMetadata(dir, nodeLabel, i).numPages,
                            getDataTypeSize(property.dataType));
                }
            }
        }
    }
}

void AdjListsLoaderHelper::saveToFile() {
    for (auto& dir : DIRS) {
        if (!description.isSingleCardinalityPerDir[dir]) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                (*dirLabelAdjLists)[dir][nodeLabel]->saveToFile();
                auto fname = RelsStore::getAdjListsIndexFname(
                    outputDirectory, description.label, nodeLabel, dir);
                (*dirLabelAdjListsMetadata)[dir][nodeLabel].saveToFile(fname);
                (*dirLabelAdjListHeaders)[dir][nodeLabel].saveToFile(fname);
            }
        }
    }
    if (description.requirePropertyLists()) {
        for (auto& dir : DIRS) {
            for (auto& nodeLabel : description.nodeLabelsPerDir[dir]) {
                for (auto i = 0u; i < description.propertyMap->size(); i++) {
                    auto& property = (*description.propertyMap)[i];
                    if ((*dirLabelPropertyIdxPropertyLists)[dir][nodeLabel][i]) {
                        (*dirLabelPropertyIdxPropertyLists)[dir][nodeLabel][i]->saveToFile();
                        auto fname = RelsStore::getRelPropertyListsFname(
                            outputDirectory, description.label, nodeLabel, dir, property.name);
                        (*dirLabelPropertyIdxPropertyListsMetadata)[dir][nodeLabel][i].saveToFile(
                            fname);
                    }
                }
            }
        }
    }
}

} // namespace loader
} // namespace graphflow
