#pragma once

#include "planner/operator/logical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace planner {

class LogicalTransaction : public LogicalOperator {
public:
    LogicalTransaction(transaction::TransactionAction transactionAction)
        : LogicalOperator{LogicalOperatorType::TRANSACTION}, transactionAction{transactionAction} {}

    inline std::string getExpressionsForPrinting() const final { return std::string(); }

    inline void computeFlatSchema() final { createEmptySchema(); }
    inline void computeFactorizedSchema() final { createEmptySchema(); }

    inline transaction::TransactionAction getTransactionAction() const { return transactionAction; }

    inline std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalTransaction>(transactionAction);
    }

private:
    transaction::TransactionAction transactionAction;
};

} // namespace planner
} // namespace kuzu
