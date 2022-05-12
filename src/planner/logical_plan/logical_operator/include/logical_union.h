#pragma once

#include "base_logical_operator.h"
#include "schema.h"

namespace graphflow {
namespace planner {

class LogicalUnion : public LogicalOperator {

public:
    explicit LogicalUnion(expression_vector expressions) : expressionsToUnion{move(expressions)} {}

    inline LogicalOperatorType getLogicalOperatorType() const override {
        return LogicalOperatorType::LOGICAL_UNION_ALL;
    }

    string getExpressionsForPrinting() const override;

    inline expression_vector getExpressionsToUnion() { return expressionsToUnion; }

    inline void addSchema(unique_ptr<Schema> schema) { schemasBeforeUnion.push_back(move(schema)); }
    inline Schema* getSchemaBeforeUnion(uint32_t idx) { return schemasBeforeUnion[idx].get(); }

    inline unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUnion>(expressionsToUnion);
    }

private:
    expression_vector expressionsToUnion;
    vector<unique_ptr<Schema>> schemasBeforeUnion;
};

} // namespace planner
} // namespace graphflow
