#pragma once

#include "create_sequence_info.h"
#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CreateSequence final : public Statement {
public:
    explicit CreateSequence(CreateSequenceInfo info)
        : Statement{common::StatementType::CREATE_SEQUENCE}, info{std::move(info)} {}

    inline const CreateSequenceInfo* getInfo() const { return &info; }

private:
    CreateSequenceInfo info;
};

} // namespace parser
} // namespace kuzu
