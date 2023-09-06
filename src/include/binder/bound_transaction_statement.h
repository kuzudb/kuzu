#pragma once

#include "bound_statement.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace binder {

class BoundTransactionStatement : public BoundStatement {
public:
    explicit BoundTransactionStatement(transaction::TransactionAction transactionAction)
        : BoundStatement{common::StatementType::TRANSACTION,
              BoundStatementResult::createEmptyResult()},
          transactionAction{transactionAction} {}

    inline transaction::TransactionAction getTransactionAction() const { return transactionAction; }

private:
    transaction::TransactionAction transactionAction;
};

} // namespace binder
} // namespace kuzu
