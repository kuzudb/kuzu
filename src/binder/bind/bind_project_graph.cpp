#include "binder/binder.h"
#include "catalog/catalog.h"
#include "common/exception/binder.h"
#include "graph/graph_entry.h"
#include "main/client_context.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::graph;

namespace kuzu {
namespace binder {

graph::GraphEntry Binder::bindProjectGraph(const ProjectGraph& projectGraph) {
    auto catalog = clientContext->getCatalog();
    auto transaction = clientContext->getTx();
    std::vector<catalog::TableCatalogEntry*> nodeEntries;
    std::vector<catalog::TableCatalogEntry*> relEntries;
    for (auto tableName : projectGraph.getTableNames()) {
        auto entry = catalog->getTableCatalogEntry(transaction, tableName);
        switch (entry->getTableType()) {
        case TableType::NODE: {
            nodeEntries.push_back(entry);
        } break;
        case TableType::REL: {
            relEntries.push_back(entry);
        } break;
        default:
            throw BinderException(stringFormat("Cannot create a subgraph with table type {}.",
                TableTypeUtils::toString(entry->getTableType())));
        }
    }
    return GraphEntry(std::move(nodeEntries), std::move(relEntries));
}

} // namespace binder
} // namespace kuzu
