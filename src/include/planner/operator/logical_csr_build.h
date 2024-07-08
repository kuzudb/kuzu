#pragma once

#include "binder/expression/rel_expression.h"
#include "common/enums/extend_direction.h"
#include "planner/operator/logical_operator.h"

using namespace kuzu::common;

namespace kuzu {
namespace planner {

class LogicalCSRBuild : public LogicalOperator {
    static constexpr LogicalOperatorType operatorType_ = LogicalOperatorType::CSR_INDEX_BUILD;

public:
    LogicalCSRBuild(std::shared_ptr<binder::Expression> boundNode,
        common::table_id_vector_t relTableIDs, std::shared_ptr<LogicalOperator> child)
        : LogicalOperator{operatorType_, std::move(child)}, boundNode{std::move(boundNode)},
          relTableIDs{relTableIDs} {}

    inline std::shared_ptr<binder::Expression> getBoundNode() const { return boundNode; }
    inline common::table_id_t getSingleRelTableID() const { return relTableIDs[0]; }
    virtual f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::string getExpressionsForPrinting() const override;

    inline std::unique_ptr<LogicalOperator> copy() override {
        return std::make_unique<LogicalCSRBuild>(boundNode, relTableIDs, children[0]->copy());
    }

private:
    std::shared_ptr<binder::Expression> boundNode;
    common::table_id_vector_t relTableIDs;
};

} // namespace planner
} // namespace kuzu
