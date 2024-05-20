#include "function/algorithm/on_disk_graph.h"

#include "common/enums/table_type.h"
#include "function/algorithm/graph.h"

using namespace kuzu::catalog;
using namespace kuzu::storage;
using namespace kuzu::main;
using namespace kuzu::common;

namespace kuzu {
namespace graph {

OnDiskGraph::OnDiskGraph(ClientContext* context, const std::vector<std::string>& tableNames)
    : Graph{context} {
    auto catalog = context->getCatalog();
    auto storage = context->getStorageManager();
    auto tx = context->getTx();
    for (auto& tableName : tableNames) {
        auto tableID = catalog->getTableID(tx, tableName);
        auto table = storage->getTable(tableID);
        auto entry = catalog->getTableCatalogEntry(tx, tableID);
        switch (entry->getTableType()) {
        case TableType::NODE: {
            auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(table);
            nodeTables.insert({tableName, nodeTable});
        } break;
        case TableType::REL: {
            auto relTable = ku_dynamic_cast<Table*, RelTable*>(table);
            relTables.insert({tableName, relTable});
        } break;
        default:
            KU_UNREACHABLE;
        }
    }
}

common::offset_t OnDiskGraph::getNumNodes() {
    common::offset_t numNodes = 0;
    for (auto& [_, nodeTable] : nodeTables) {
        numNodes += nodeTable->getNumTuples(context->getTx());
    }
    return numNodes;
}

common::offset_t OnDiskGraph::getNumEdges() {
    common::offset_t numEdges = 0;
    for (auto& [_, relTable] : relTables) {
        numEdges += relTable->getNumTuples(context->getTx());
    }
    return numEdges;
}

uint64_t OnDiskGraph::getFwdDegreeOffset(common::offset_t offset) {
    auto dummyTxn = transaction::Transaction::getDummyReadOnlyTrx();
    uint64_t totalFwdDegree = 0LU;
    for (auto& [_, relTable] : relTables) {
        totalFwdDegree += relTable->getDirectedTableData(common::RelDataDirection::FWD)
                              ->getNodeRels(dummyTxn.get(), offset);
    }
    return totalFwdDegree;
}

} // namespace graph
} // namespace kuzu
