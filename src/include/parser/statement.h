#pragma once

#include "common/statement_type.h"

namespace kuzu {
namespace parser {

class Statement {
public:
    explicit Statement(common::StatementType statementType) : statementType{statementType} {}

    virtual ~Statement() = default;

    inline common::StatementType getStatementType() const { return statementType; }

    inline void enableExplain() { explain = true; }
    inline bool isExplain() const { return explain; }

    inline void enableProfile() { profile = true; }
    inline bool isProfile() const { return profile; }

private:
    common::StatementType statementType;
    // If explain is enabled, we do not execute query but return physical plan only.
    bool explain = false;
    bool profile = false;
};

} // namespace parser
} // namespace kuzu
