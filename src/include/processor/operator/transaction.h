#pragma once

#include "processor/operator/physical_operator.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace processor {

class Transaction : public PhysicalOperator {
    static constexpr PhysicalOperatorType type_ = PhysicalOperatorType::TRANSACTION;

public:
    Transaction(transaction::TransactionAction transactionAction, uint32_t id,
        std::unique_ptr<OPPrintInfo> printInfo)
        : PhysicalOperator{type_, id, std::move(printInfo)}, transactionAction{transactionAction},
          hasExecuted{false} {}

    bool isSource() const final { return true; }
    bool isParallel() const final { return false; }

    void initLocalStateInternal(ResultSet* /*resultSet_*/, ExecutionContext* /*context*/) final {
        hasExecuted = false;
    }

    bool getNextTuplesInternal(ExecutionContext* context) final;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<Transaction>(transactionAction, id, printInfo->copy());
    }

private:
    void validateActiveTransaction(const transaction::TransactionContext& context) const;

private:
    transaction::TransactionAction transactionAction;
    bool hasExecuted;
};

} // namespace processor
} // namespace kuzu
