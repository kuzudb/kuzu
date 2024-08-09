#pragma once

#include "planner/operator/logical_operator.h"

namespace kuzu {
namespace planner {

class LogicalUnwind : public LogicalOperator {
public:
    LogicalUnwind(std::shared_ptr<binder::Expression> inExpr,
        std::shared_ptr<binder::Expression> outExpr, std::shared_ptr<binder::Expression> idExpr,
        std::shared_ptr<LogicalOperator> childOperator, std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{LogicalOperatorType::UNWIND, std::move(childOperator),
              std::move(printInfo)},
          inExpr{std::move(inExpr)}, outExpr{std::move(outExpr)}, idExpr{std::move(idExpr)} {}

    f_group_pos_set getGroupsPosToFlatten();

    void computeFactorizedSchema() override;
    void computeFlatSchema() override;

    std::shared_ptr<binder::Expression> getInExpr() const { return inExpr; }
    std::shared_ptr<binder::Expression> getOutExpr() const { return outExpr; }
    bool hasIDExpr() const { return idExpr != nullptr; }
    std::shared_ptr<binder::Expression> getIDExpr() const { return idExpr; }

    std::string getExpressionsForPrinting() const override { return inExpr->toString(); }

    std::unique_ptr<LogicalOperator> copy() override {
        return make_unique<LogicalUnwind>(inExpr, outExpr, idExpr, children[0]->copy(),
            printInfo->copy());
    }

private:
    std::shared_ptr<binder::Expression> inExpr;
    std::shared_ptr<binder::Expression> outExpr;
    std::shared_ptr<binder::Expression> idExpr;
};

} // namespace planner
} // namespace kuzu
