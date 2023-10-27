#pragma once

#include "catalog/catalog.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    QueryGraphLabelAnalyzer(const catalog::Catalog& catalog) : catalog{catalog} {}

    void pruneLabel(const QueryGraph& graph);

private:
    void pruneNode(const QueryGraph& graph, NodeExpression& node);
    void pruneRel(RelExpression& rel);

private:
    const catalog::Catalog& catalog;
};

} // namespace binder
} // namespace kuzu
