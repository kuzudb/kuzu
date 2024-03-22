#pragma once

#include "catalog/catalog.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    // TODO(Jiamin): remove catalog
    explicit QueryGraphLabelAnalyzer(
        const catalog::Catalog& catalog, const main::ClientContext& clientContext)
        : catalog{catalog}, clientContext{clientContext} {}

    void pruneLabel(const QueryGraph& graph);

private:
    void pruneNode(const QueryGraph& graph, NodeExpression& node);
    void pruneRel(RelExpression& rel);

private:
    const catalog::Catalog& catalog;
    const main::ClientContext& clientContext;
};

} // namespace binder
} // namespace kuzu
