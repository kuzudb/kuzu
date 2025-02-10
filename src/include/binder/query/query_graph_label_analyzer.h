#pragma once

#include "common/enums/rel_direction.h"
#include "main/client_context.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    explicit QueryGraphLabelAnalyzer(const main::ClientContext& clientContext, bool throwOnViolate)
        : throwOnViolate{throwOnViolate}, clientContext{clientContext} {
        tx = clientContext.getTransaction();
        catalog = clientContext.getCatalog();
    }

    void pruneLabel(QueryGraph& graph) const;

private:
    void pruneNode(const QueryGraph& graph, NodeExpression& node) const;
    void pruneRel(RelExpression& rel) const;

    common::table_id_set_t collectRelNodes(const common::RelDataDirection direction,
        std::vector<catalog::TableCatalogEntry*> relEntries) const;

    std::pair<std::vector<common::table_id_set_t>, std::vector<common::table_id_set_t>>
    pruneRecursiveRel(const std::vector<catalog::TableCatalogEntry*>& relEntries,
        const common::table_id_set_t srcTableIDSet, const common::table_id_set_t dstTableIDSet,
        size_t lowerBound, size_t upperBound, RelDirectionType relDirectionType) const;

    std::vector<catalog::TableCatalogEntry*> pruneNonRecursiveRel(
        const std::vector<catalog::TableCatalogEntry*>& relEntries,
        const common::table_id_set_t& srcTableIDSet, const common::table_id_set_t& dstTableIDSet,
        const RelDirectionType directionType) const;

    std::vector<catalog::TableCatalogEntry*> getTableCatalogEntries(
        common::table_id_set_t tableIDs) const;

    std::vector<common::table_id_set_t> pruneRecursiveRel(
        const std::unordered_map<common::table_id_t,
            std::unordered_map<common::table_id_t, common::table_id_vector_t>>& graph,
        const common::table_id_set_t& startTableIDSet, const common::table_id_set_t& endTableIDSet,
        size_t lowerBound, size_t upperBound) const;

    std::vector<common::table_id_t> getNodeTableIDs() const;

    std::unordered_set<catalog::TableCatalogEntry*> mergeTableIDs(
        const std::vector<common::table_id_set_t>& v1,
        const std::vector<common::table_id_set_t>& v2) const;

private:
    bool throwOnViolate;
    const main::ClientContext& clientContext;
    transaction::Transaction* tx;
    catalog::Catalog* catalog;
};

} // namespace binder
} // namespace kuzu
