#pragma once

#include "main/client_context.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    explicit QueryGraphLabelAnalyzer(const main::ClientContext& clientContext, bool throwOnViolate)
        : throwOnViolate{throwOnViolate}, clientContext{clientContext} {}

    void pruneLabel(QueryGraph& graph);

private:
    void pruneNode(const QueryGraph& graph, NodeExpression& node);
    void pruneRel(RelExpression& rel);

private:
    bool throwOnViolate;
    const main::ClientContext& clientContext;
};

} // namespace binder
} // namespace kuzu
