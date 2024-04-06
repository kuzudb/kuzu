#pragma once

#include <string>

#include "parser/statement.h"

namespace kuzu {
namespace parser {

class CommentOn : public Statement {
public:
    explicit CommentOn(std::string table, std::string comment)
        : Statement{common::StatementType::COMMENT_ON}, table{std::move(table)},
          comment{std::move(comment)} {}

    inline std::string getTable() const { return table; }

    inline std::string getComment() const { return comment; }

private:
    std::string table, comment;
};

} // namespace parser
} // namespace kuzu
