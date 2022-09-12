#include "src/storage/store/include/rel_table.h"

#include "spdlog/spdlog.h"

using namespace graphflow::catalog;

namespace graphflow {
namespace storage {

RelTable::RelTable(const Catalog& catalog, const vector<uint64_t>& maxNodeOffsetsPerTable,
    table_id_t tableID, BufferManager& bufferManager, bool isInMemoryMode, WAL* wal)
    : logger{LoggerUtils::getOrCreateSpdLogger("storage")}, tableID{tableID}, isInMemoryMode{
                                                                                  isInMemoryMode} {
    loadColumnsAndListsFromDisk(catalog, maxNodeOffsetsPerTable, bufferManager, wal);
}

// Todo(Ziyi): We should remove this function and put this back to constructor once we implement
// transaction for copycsv.
void RelTable::loadColumnsAndListsFromDisk(const catalog::Catalog& catalog,
    const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager, WAL* wal) {
    initAdjColumnOrLists(catalog, maxNodeOffsetsPerTable, bufferManager, wal);
    initPropertyListsAndColumns(catalog, bufferManager, wal);
}

vector<AdjLists*> RelTable::getAdjListsForNodeTable(table_id_t tableID) {
    vector<AdjLists*> retVal;
    auto it = adjLists[FWD].find(tableID);
    if (it != adjLists[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjLists[BWD].find(tableID);
    if (it != adjLists[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

vector<AdjColumn*> RelTable::getAdjColumnsForNodeTable(table_id_t tableID) {
    vector<AdjColumn*> retVal;
    auto it = adjColumns[FWD].find(tableID);
    if (it != adjColumns[FWD].end()) {
        retVal.push_back(it->second.get());
    }
    it = adjColumns[BWD].find(tableID);
    if (it != adjColumns[BWD].end()) {
        retVal.push_back(it->second.get());
    }
    return retVal;
}

void RelTable::initAdjColumnOrLists(const Catalog& catalog,
    const vector<uint64_t>& maxNodeOffsetsPerTable, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing AdjColumns and AdjLists for rel {}.", tableID);
    adjColumns = vector<table_adj_columns_map_t>{2};
    adjLists = vector<table_adj_lists_map_t>{2};
    for (auto relDirection : REL_DIRECTIONS) {
        const auto& nodeTableIDs =
            catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                tableID, relDirection);
        const auto& nbrNodeTableIDs =
            catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(
                tableID, !relDirection);
        for (auto nodeTableID : nodeTableIDs) {
            NodeIDCompressionScheme nodeIDCompressionScheme(nbrNodeTableIDs);
            logger->debug("DIRECTION {} nodeTableForAdjColumnAndProperties {} relTable {} "
                          "nodeIDCompressionScheme: commonTableID: {}",
                relDirection, nodeTableID, tableID, nodeIDCompressionScheme.getCommonTableID());
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    tableID, relDirection)) {
                // Add adj column.
                auto storageStructureIDAndFName = StorageUtils::getAdjColumnStructureIDAndFName(
                    wal->getDirectory(), tableID, nodeTableID, relDirection);
                auto adjColumn = make_unique<AdjColumn>(storageStructureIDAndFName, bufferManager,
                    nodeIDCompressionScheme, isInMemoryMode, wal);
                adjColumns[relDirection].emplace(nodeTableID, move(adjColumn));
            } else {
                // Add adj list.
                auto adjList = make_unique<AdjLists>(
                    StorageUtils::getAdjListsStructureIDAndFName(
                        wal->getDirectory(), tableID, nodeTableID, relDirection),
                    bufferManager, nodeIDCompressionScheme, isInMemoryMode, wal);
                adjLists[relDirection].emplace(nodeTableID, move(adjList));
            }
        }
    }
    logger->info("Initializing AdjColumns and AdjLists for rel {} done.", tableID);
}

void RelTable::initPropertyListsAndColumns(
    const Catalog& catalog, BufferManager& bufferManager, WAL* wal) {
    logger->info("Initializing PropertyLists and PropertyColumns for rel {}.", tableID);
    propertyLists = vector<table_property_lists_map_t>{2};
    propertyColumns = unordered_map<table_id_t, vector<unique_ptr<Column>>>{};
    if (!catalog.getReadOnlyVersion()->getRelProperties(tableID).empty()) {
        for (auto relDirection : REL_DIRECTIONS) {
            if (catalog.getReadOnlyVersion()->isSingleMultiplicityInDirection(
                    tableID, relDirection)) {
                initPropertyColumnsForRelTable(catalog, relDirection, bufferManager, wal);
            } else {
                initPropertyListsForRelTable(catalog, relDirection, bufferManager, wal);
            }
        }
    }
    logger->info("Initializing PropertyLists and PropertyColumns for rel {} Done.", tableID);
}

void RelTable::initPropertyColumnsForRelTable(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyColumns: relTable {}", tableID);
    for (auto& nodeTableID :
        catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(tableID, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(tableID);
        propertyColumns[nodeTableID].resize(properties.size());
        for (auto& property : properties) {
            auto storageStructureIDAndFName = StorageUtils::getRelPropertyColumnStructureIDAndFName(
                wal->getDirectory(), tableID, nodeTableID, relDirection, property.name);
            logger->debug(
                "DIR {} nodeIDForAdjColumnAndProperties {} propertyIdx {} type {} name `{}`",
                relDirection, nodeTableID, property.propertyID, property.dataType.typeID,
                property.name);
            propertyColumns[nodeTableID][property.propertyID] = ColumnFactory::getColumn(
                storageStructureIDAndFName, property.dataType, bufferManager, isInMemoryMode, wal);
        }
    }
    logger->debug("Initializing PropertyColumns done.");
}

void RelTable::initPropertyListsForRelTable(
    const Catalog& catalog, RelDirection relDirection, BufferManager& bufferManager, WAL* wal) {
    logger->debug("Initializing PropertyLists for rel {}", tableID);
    for (auto& nodeTableID :
        catalog.getReadOnlyVersion()->getNodeTableIDsForRelTableDirection(tableID, relDirection)) {
        auto& properties = catalog.getReadOnlyVersion()->getRelProperties(tableID);
        auto adjListsHeaders = adjLists[relDirection].at(nodeTableID)->getHeaders();
        propertyLists[relDirection].emplace(
            nodeTableID, vector<unique_ptr<Lists>>(properties.size()));
        for (auto propertyIdx = 0u; propertyIdx < properties.size(); propertyIdx++) {
            logger->debug("relDirection {} nodeTableForAdjColumnAndProperties {} propertyIdx {} "
                          "type {} name `{}`",
                relDirection, nodeTableID, properties[propertyIdx].propertyID,
                properties[propertyIdx].dataType.typeID, properties[propertyIdx].name);
            auto propertyList = ListsFactory::getLists(
                StorageUtils::getRelPropertyListsStructureIDAndFName(wal->getDirectory(), tableID,
                    nodeTableID, relDirection, properties[propertyIdx]),
                properties[propertyIdx].dataType, adjListsHeaders, bufferManager, isInMemoryMode,
                wal);
            propertyLists[relDirection].at(nodeTableID)[propertyIdx] = move(propertyList);
        }
    }
    logger->debug("Initializing PropertyLists for rel {} done.", tableID);
}

} // namespace storage
} // namespace graphflow
