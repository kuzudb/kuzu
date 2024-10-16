#pragma once

#include "common/exception/runtime.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

/*
 * Semi mask is always collecting offset but maybe constructed from different type of Vectors
 *
 * NODE
 * offsets are collected from value vector of type NODE_ID (INTERNAL_ID)
 *
 * PATH
 * offsets are collected from value vector of type PATH (LIST[INTERNAL_ID]). This is a fast-path
 * code used when scanning properties along the path.
 * */
enum class SemiMaskConstructionType : uint8_t {
    NODE = 0,
    PATH = 1,
};

class LogicalSemiMasker : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::SEMI_MASKER;

public:
    LogicalSemiMasker(SemiMaskConstructionType type, std::shared_ptr<binder::Expression> key,
        std::vector<common::table_id_t> nodeTableIDs, std::vector<LogicalOperator*> ops,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{type_, std::move(child)}, type{type}, key{std::move(key)},
          nodeTableIDs{std::move(nodeTableIDs)}, ops{std::move(ops)} {}

    void computeFactorizedSchema() override { copyChildSchema(0); }
    void computeFlatSchema() override { copyChildSchema(0); }

    std::string getExpressionsForPrinting() const override { return key->toString(); }

    SemiMaskConstructionType getType() const { return type; }
    std::shared_ptr<binder::Expression> getKey() const { return key; }
    std::vector<common::table_id_t> getNodeTableIDs() const { return nodeTableIDs; }
    std::vector<LogicalOperator*> getOperators() const { return ops; }

    std::unique_ptr<LogicalOperator> copy() override {
        throw common::RuntimeException("LogicalSemiMasker::copy() should not be called.");
    }

private:
    SemiMaskConstructionType type;
    std::shared_ptr<binder::Expression> key;
    std::vector<common::table_id_t> nodeTableIDs;
    // Operators accepting semi masker
    std::vector<LogicalOperator*> ops;
};

} // namespace planner
} // namespace kuzu
