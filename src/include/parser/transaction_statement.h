#pragma once

#include "statement.h"
#include "transaction/transaction_action.h"

namespace kuzu {
namespace parser {

class TransactionStatement : public Statement {
public:
    TransactionStatement(transaction::TransactionAction transactionAction)
        : Statement{common::StatementType::TRANSACTION}, transactionAction{transactionAction} {}

    inline transaction::TransactionAction getTransactionAction() const { return transactionAction; }

private:
    transaction::TransactionAction transactionAction;
};

} // namespace parser
} // namespace kuzu
