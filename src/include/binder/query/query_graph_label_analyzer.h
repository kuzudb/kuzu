#pragma once

#include "main/client_context.h"
#include "query_graph.h"

namespace kuzu {
namespace binder {

class QueryGraphLabelAnalyzer {
public:
    // TODO(Jiamin): remove catalog
    explicit QueryGraphLabelAnalyzer(const main::ClientContext& clientContext)
        : clientContext{clientContext} {}

    void pruneLabel(const QueryGraph& graph);

private:
    void pruneNode(const QueryGraph& graph, NodeExpression& node);
    void pruneRel(RelExpression& rel);

private:
    const main::ClientContext& clientContext;
};

} // namespace binder
} // namespace kuzu
