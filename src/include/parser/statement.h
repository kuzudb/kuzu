#pragma once

#include "common/enums/statement_type.h"

namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType) : statementType{statementType} {}

    virtual ~Statement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

    inline bool requireTx() {
        switch (statementType) {
        case common::StatementType::TRANSACTION:
            return false;
        default:
            return true;
        }
    }

private:
    common::StatementType statementType;
};

} // namespace parser
} // namespace kuzu
