#pragma once

#include "processor/operator/physical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace processor {

class Transaction : public PhysicalOperator {
public:
    Transaction(transaction::TransactionAction transactionAction, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::TRANSACTION, id, paramsString},
          transactionAction{transactionAction}, hasExecuted{false} {}

    bool isSource() const final { return true; }
    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) final {
        hasExecuted = false;
    }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<Transaction>(transactionAction, id, paramsString);
    }

private:
    void validateActiveTransaction(const transaction::TransactionContext& context) const;

private:
    transaction::TransactionAction transactionAction;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
