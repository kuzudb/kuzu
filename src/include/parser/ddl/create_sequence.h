#pragma once

#include "create_sequence_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateSequence final : public Statement {
public:
    explicit CreateSequence(CreateSequenceInfo info)
        : Statement{common::StatementType::CREATE_SEQUENCE}, info{std::move(info)} {}

    CreateSequenceInfo getInfo() const { return info.copy(); }

private:
    CreateSequenceInfo info;
};

} // namespace parser
} // namespace kuzu
