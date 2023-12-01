#pragma once

#include "common/exception/runtime.h"
#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

enum class SemiMaskType : uint8_t {
    NODE = 0,
    PATH = 1,
};

class LogicalSemiMasker : public LogicalOperator {
public:
    LogicalSemiMasker(SemiMaskType type, std::shared_ptr<binder::Expression> key,
        std::vector<common::table_id_t> nodeTableIDs, std::vector<LogicalOperator*> ops,
        std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{LogicalOperatorType::SEMI_MASKER, std::move(child)}, type{type},
          key{std::move(key)}, nodeTableIDs{std::move(nodeTableIDs)}, ops{std::move(ops)} {}

    inline void computeFactorizedSchema() override { copyChildSchema(0); }
    inline void computeFlatSchema() override { copyChildSchema(0); }

    inline std::string getExpressionsForPrinting() const override { return key->toString(); }

    inline SemiMaskType getType() const { return type; }
    inline std::shared_ptr<binder::Expression> getKey() const { return key; }
    inline std::vector<common::table_id_t> getNodeTableIDs() const { return nodeTableIDs; }
    inline std::vector<LogicalOperator*> getOperators() const { return ops; }

    inline std::unique_ptr<LogicalOperator> copy() override {
        throw common::RuntimeException("LogicalSemiMasker::copy() should not be called.");
    }

private:
    SemiMaskType type;
    std::shared_ptr<binder::Expression> key;
    std::vector<common::table_id_t> nodeTableIDs;
    std::vector<LogicalOperator*> ops;
};

} // namespace planner
} // namespace kuzu
