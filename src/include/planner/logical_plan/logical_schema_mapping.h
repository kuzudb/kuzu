#pragma once

#include <utility>

#include "logical_operator.h"
#include "planner/logical_plan/factorization/flatten_resolver.h"

using namespace kuzu::planner::factorization;

namespace kuzu {
namespace planner {

class LogicalSchemaMapping : public LogicalOperator {
public:
    LogicalSchemaMapping(
        binder::expression_map<std::shared_ptr<binder::Expression>> expressionsMapping,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator(LogicalOperatorType::SCHEMA_MAPPING, std::move(child)),
          expressionsMapping{std::move(expressionsMapping)} {}

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    inline f_group_pos_set getGroupsPosToFlatten() {
        auto childSchema = children[0]->getSchema();
        return FlattenAll::getGroupsPosToFlatten(childSchema->getGroupsPosInScope(), childSchema);
    }

    inline std::string getExpressionsForPrinting() const override {
        std::string result;
        for (const auto& [key, value] : expressionsMapping) {
            result += key->toString() + " -> " + value->toString() + ",";
        }
        return result;
    }

    inline std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalSchemaMapping>(expressionsMapping, children[0]->copy());
    }

private:
    binder::expression_map<std::shared_ptr<binder::Expression>> expressionsMapping;
};

} // namespace planner
} // namespace kuzu
