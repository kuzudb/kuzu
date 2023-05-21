#include "storage/store/rels_store.h"

using namespace kuzu::catalog;
using namespace kuzu::common;

namespace kuzu {
namespace storage {

RelsStore::RelsStore(const Catalog& catalog, MemoryManager& memoryManager, WAL* wal)
    : relsStatistics{wal->getDirectory()}, wal{wal} {
    for (auto& tableIDSchema : catalog.getReadOnlyVersion()->getRelTableSchemas()) {
        relTables[tableIDSchema.first] =
            std::make_unique<RelTable>(catalog, tableIDSchema.first, memoryManager, wal);
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
