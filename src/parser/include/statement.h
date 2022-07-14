#pragma once

namespace graphflow {
namespace parser {

enum class StatementType : uint8_t {
    QUERY = 0,
    CreateNodeClause = 1,
};

class Statement {
public:
    explicit Statement(StatementType statementType) : statementType{statementType} {}

    inline StatementType getStatementType() const { return statementType; }

private:
    StatementType statementType;
};

} // namespace parser
} // namespace graphflow
