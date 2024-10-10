#pragma once

#include "planner/operator/logical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace planner {

class LogicalTransaction final : public LogicalOperator {
    static constexpr LogicalOperatorType type_ = LogicalOperatorType::TRANSACTION;

public:
    explicit LogicalTransaction(transaction::TransactionAction transactionAction)
        : LogicalOperator{type_}, transactionAction{transactionAction} {}

    std::string getExpressionsForPrinting() const override { return std::string(); }

    void computeFlatSchema() override { createEmptySchema(); }
    void computeFactorizedSchema() override { createEmptySchema(); }

    transaction::TransactionAction getTransactionAction() const { return transactionAction; }

    std::unique_ptr<LogicalOperator> copy() final {
        return std::make_unique<LogicalTransaction>(transactionAction);
    }

private:
    transaction::TransactionAction transactionAction;
};

} // namespace planner
} // namespace kuzu
