#pragma once

#include "planner/operator/logical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace planner {

class LogicalTransaction : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::TRANSACTION;

public:
    LogicalTransaction(transaction::TransactionAction transactionAction,
        std::unique_ptr<OPPrintInfo> printInfo)
        : LogicalOperator{type_, std::move(printInfo)}, transactionAction{transactionAction} {}

    std::string getExpressionsForPrinting() const final { return std::string(); }

    void computeFlatSchema() final { createEmptySchema(); }
    void computeFactorizedSchema() final { createEmptySchema(); }

    transaction::TransactionAction getTransactionAction() const { return transactionAction; }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalTransaction>(transactionAction, printInfo->copy());
    }

private:
    transaction::TransactionAction transactionAction;
};

} // namespace planner
} // namespace kuzu
