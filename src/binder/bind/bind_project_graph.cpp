#include "binder/binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "graph/graph_entry.h"
#include "parser/project_graph.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::graph;

namespace kuzu {
namespace binder {

graph::GraphEntry Binder::bindProjectGraph(const ProjectGraph& projectGraph) {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    std::vector<common::table_id_t> nodeTableIDs;
    std::vector<common::table_id_t> relTableIDs;
    for (auto tableName : projectGraph.getTableNames()) {
        auto tableID = catalog->getTableID(transaction, tableName);
        auto entry = catalog->getTableCatalogEntry(transaction, tableID);
        switch (entry->getTableType()) {
        case TableType::NODE: {
            nodeTableIDs.push_back(tableID);
        } break;
        case TableType::REL: {
            relTableIDs.push_back(tableID);
        } break;
        default:
            throw BinderException(stringFormat("Cannot create a subgraph with table type {}.",
                TableTypeUtils::toString(entry->getTableType())));
        }
    }
    if (nodeTableIDs.size() != 1 || relTableIDs.size() != 1) {
        throw BinderException(
            "A subgraph must have exactly one node table and one relationship table.");
    }
    return GraphEntry(std::move(nodeTableIDs), std::move(relTableIDs));
}

} // namespace binder
} // namespace kuzu
