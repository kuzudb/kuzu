#include "storage/store/rels_store.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

RelsStore::RelsStore(BMFileHandle* metadataFH, AccessMode accessMode, const Catalog& catalog,
    MemoryManager& memoryManager, WAL* wal)
    : wal{wal} {
    relsStatistics = std::make_unique<RelsStoreStats>(metadataFH, wal->getDirectory());
    for (auto& relTableSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        relTables.emplace(
            relTableSchema->tableID, std::make_unique<RelTable>(catalog, relTableSchema->tableID,
                                         memoryManager, wal, accessMode));
    }
}

std::pair<std::vector<AdjLists*>, std::vector<Column*>> RelsStore::getAdjListsAndColumns(
    const table_id_t boundTableID) const {
    std::vector<AdjLists*> adjListsRetVal;
    for (auto& [_, relTable] : relTables) {
        auto adjListsForRel = relTable->getAllAdjLists(boundTableID);
        adjListsRetVal.insert(adjListsRetVal.end(), adjListsForRel.begin(), adjListsForRel.end());
    }
    std::vector<Column*> adjColumnsRetVal;
    for (auto& [_, relTable] : relTables) {
        auto adjColumnsForRel = relTable->getAllAdjColumns(boundTableID);
        adjColumnsRetVal.insert(
            adjColumnsRetVal.end(), adjColumnsForRel.begin(), adjColumnsForRel.end());
    }
    return std::make_pair(adjListsRetVal, adjColumnsRetVal);
}

} // namespace storage
} // namespace kuzu
