#include "src/storage/store/include/rel_table.h"

#include "spdlog/spdlog.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

RelTable::RelTable(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerLabel,
    label_t relLabel, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, relLabel{relLabel},
      isInMemoryMode{isInMemoryMode} {
    loadColumnsAndListsFromDisk(catalog, maxNodeOffsetsPerLabel, bufferManager, wal);
}

// Todo(Ziyi): We should remove this function and put this back to constructor once we implement
// transaction for copycsv.
void RelTable::loadColumnsAndListsFromDisk(const catalog::Catalog& catalog,
    const vector<uint64_t>& maxNodeOffsetsPerLabel, BufferManager& bufferManager, WAL* wal) {
    initAdjColumnOrLists(catalog, maxNodeOffsetsPerLabel, bufferManager, wal);
    initPropertyListsAndColumns(catalog, bufferManager, wal);
}

vector<AdjLists*> RelTable::getAdjListsForNodeLabel(label_t nodeLabel) {
    vector<AdjLists*> retVal;
    auto it = adjLists[FWD].find(nodeLabel);
    if (it != adjLists[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjLists[BWD].find(nodeLabel);
    if (it != adjLists[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

vector<AdjColumn*> RelTable::getAdjColumnsForNodeLabel(label_t nodeLabel) {
    vector<AdjColumn*> retVal;
    auto it = adjColumns[FWD].find(nodeLabel);
    if (it != adjColumns[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjColumns[BWD].find(nodeLabel);
    if (it != adjColumns[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

void RelTable::initAdjColumnOrLists(const Catalog& catalog,
    const vector<uint64_t>& maxNodeOffsetsPerLabel, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing AdjColumns and AdjLists for rel {}.", relLabel);
    adjColumns = vector<label_adj_columns_map_t>{2};
    adjLists = vector<label_adj_lists_map_t>{2};
    for (auto relDirection : REL_DIRECTIONS) {
        const auto& nodeLabels =
            catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(relLabel, relDirection);
        const auto& nbrNodeLabels = catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(
            relLabel, !relDirection);
        for (auto nodeLabel : nodeLabels) {
            NodeIDCompressionScheme nodeIDCompressionScheme(nbrNodeLabels, maxNodeOffsetsPerLabel);
            logger->debug("DIRECTION {} nodeLabelForAdjColumnAndProperties {} relLabel {} "
                          "compressionScheme {},{},{}",
                relDirection, nodeLabel, relLabel, nodeIDCompressionScheme.getNumBytesForLabel(),
                nodeIDCompressionScheme.getNumBytesForOffset(),
                nodeIDCompressionScheme.getCommonLabel());
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    relLabel, relDirection)) {
                // Add adj column.
                auto storageStructureIDAndFName = StorageUtils::getAdjColumnStructureIDAndFName(
                    wal->getDirectory(), relLabel, nodeLabel, relDirection);
                auto adjColumn = make_unique<AdjColumn>(storageStructureIDAndFName, bufferManager,
                    nodeIDCompressionScheme, isInMemoryMode, wal);
                adjColumns[relDirection].emplace(nodeLabel, move(adjColumn));
            } else {
                // Add adj list.
                auto adjList = make_unique<AdjLists>(
                    StorageUtils::getAdjListsStructureIDAndFName(
                        wal->getDirectory(), relLabel, nodeLabel, relDirection),
                    bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal);
                adjLists[relDirection].emplace(nodeLabel, move(adjList));
            }
        }
    }
    logger->info("Initializing AdjColumns and AdjLists for rel {} done.", relLabel);
}

void RelTable::initPropertyListsAndColumns(
    const Catalog& catalog, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing PropertyLists and PropertyColumns for rel {}.", relLabel);
    propertyLists = vector<label_property_lists_map_t>{2};
    propertyColumns = unordered_map<label_t, vector<unique_ptr<Column>>>{};
    if (!catalog.getReadOnlyVersion()->getRelProperties(relLabel).empty()) {
        for (auto relDirection : REL_DIRECTIONS) {
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    relLabel, relDirection)) {
                initPropertyColumnsForRelLabel(catalog, relDirection, bufferManager, wal);
            } else {
                initPropertyListsForRelLabel(catalog, relDirection, bufferManager, wal);
            }
        }
    }
    logger->info("Initializing PropertyLists and PropertyColumns for rel {} Done.", relLabel);
}

void RelTable::initPropertyColumnsForRelLabel(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyColumns: relLabel {}", relLabel);
    for (auto& nodeLabel :
        catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(relLabel, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(relLabel);
        propertyColumns[nodeLabel].resize(properties.size());
        for (auto& property : properties) {
            auto storageStructureIDAndFName = StorageUtils::getRelPropertyColumnStructureIDAndFName(
                wal->getDirectory(), relLabel, nodeLabel, relDirection, property.name);
            logger->debug(
                "DIR {} nodeLabelForAdjColumnAndProperties {} propertyIdx {} type {} name `{}`",
                relDirection, nodeLabel, property.propertyID, property.dataType.typeID,
                property.name);
            propertyColumns[nodeLabel][property.propertyID] = ColumnFactory::getColumn(
                storageStructureIDAndFName, property.dataType, bufferManager, isInMemoryMode, wal);
        }
    }
    logger->debug("Initializing PropertyColumns done.");
}

void RelTable::initPropertyListsForRelLabel(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyLists for rel {}", relLabel);
    for (auto& nodeLabel :
        catalog.getReadOnlyVersion()->getNodeLabelsForRelLabelDirection(relLabel, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(relLabel);
        auto adjListsHeaders = adjLists[relDirection].at(nodeLabel)->getHeaders();
        propertyLists[relDirection].emplace(
            nodeLabel, vector<unique_ptr<Lists>>(properties.size()));
        for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
            logger->debug("relDirection {} nodeLabelForAdjColumnAndProperties {} propertyIdx {} "
                          "type {} name `{}`",
                relDirection, nodeLabel, properties[propertyIdx].propertyID,
                properties[propertyIdx].dataType.typeID, properties[propertyIdx].name);
            auto propertyList = ListsFactory::getLists(
                StorageUtils::getRelPropertyListsStructureIDAndFName(wal->getDirectory(), relLabel,
                    nodeLabel, relDirection, properties[propertyIdx]),
                properties[propertyIdx].dataType, adjListsHeaders, bufferManager, isInMemoryMode,
                wal);
            propertyLists[relDirection].at(nodeLabel)[propertyIdx] = move(propertyList);
        }
    }
    logger->debug("Initializing PropertyLists for rel {} done.", relLabel);
}

} // namespace storage
} // namespace graphflow
