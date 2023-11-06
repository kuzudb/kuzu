#pragma once

#include "common/enums/statement_type.h"

namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType) : statementType{statementType} {}

    virtual ~Statement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

    inline void enableProfile() { profile = true; }
    inline bool isProfile() const { return profile; }

private:
    common::StatementType statementType;
    bool profile = false;
};

} // namespace parser
} // namespace kuzu
