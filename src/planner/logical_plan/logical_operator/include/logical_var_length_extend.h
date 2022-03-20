#pragma once

#include "base_logical_operator.h"
#include "logical_extend.h"

using namespace graphflow::binder;

namespace graphflow {
namespace planner {

class LogicalVarLengthExtend : public LogicalExtend {

public:
    LogicalVarLengthExtend(shared_ptr<RelExpression> queryRel, Direction direction,
        shared_ptr<LogicalOperator> child, expression_vector expressionsToMaterialize,
        unique_ptr<Schema> schema)
        : LogicalExtend{move(queryRel), direction, move(child)},
          expressionsToMaterialize{move(expressionsToMaterialize)}, schema{move(schema)} {}

    LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_VAR_LENGTH_EXTEND;
    }

    expression_vector getExpressionsToCollect() const { return expressionsToMaterialize; };

    unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalVarLengthExtend>(
            queryRel, direction, children[0]->copy(), expressionsToMaterialize, schema->copy());
    }

    Schema* getSchema() const { return schema.get(); }

private:
    expression_vector expressionsToMaterialize;
    unique_ptr<Schema> schema;
};

} // namespace planner
} // namespace graphflow
