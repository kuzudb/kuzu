#pragma once

#include "binder/query/reading_clause/bound_reading_clause.h"
#include "function/function.h"
#include "graph/graph_entry.h"

namespace kuzu {
namespace function {
class GDSAlgorithm;
struct GDSBindData;
} // namespace function
namespace binder {

struct BoundGDSCallInfo {
    std::unique_ptr<function::Function> func;
    graph::GraphEntry graphEntry;
    std::shared_ptr<Expression> srcNodeIDExpression;
    expression_vector outExpressions;

    BoundGDSCallInfo(std::unique_ptr<function::Function> func, graph::GraphEntry graphEntry,
        std::shared_ptr<Expression> srcNodeIDExpression, expression_vector outExpressions)
        : func{std::move(func)}, graphEntry{std::move(graphEntry)},
          srcNodeIDExpression{std::move(srcNodeIDExpression)},
          outExpressions{std::move(outExpressions)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(BoundGDSCallInfo);

    const function::GDSAlgorithm* getGDS() const;
    const function::GDSBindData* getBindData() const;

private:
    BoundGDSCallInfo(const BoundGDSCallInfo& other)
        : func{other.func->copy()}, graphEntry{other.graphEntry.copy()},
          srcNodeIDExpression{other.srcNodeIDExpression}, outExpressions{other.outExpressions} {}
};

class BoundGDSCall : public BoundReadingClause {
    static constexpr common::ClauseType clauseType_ = common::ClauseType::GDS_CALL;

public:
    explicit BoundGDSCall(BoundGDSCallInfo info)
        : BoundReadingClause{clauseType_}, info{std::move(info)} {}

    const BoundGDSCallInfo& getInfo() const { return info; }

private:
    BoundGDSCallInfo info;
};

} // namespace binder
} // namespace kuzu
