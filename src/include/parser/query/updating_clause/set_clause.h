#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class SetClause : public UpdatingClause {
public:
    SetClause() : UpdatingClause{common::ClauseType::SET} {};

    inline void addSetItem(parsed_expression_pair setItem) {
        setItems.push_back(std::move(setItem));
    }
    inline const std::vector<parsed_expression_pair>& getSetItemsRef() const { return setItems; }

private:
    std::vector<parsed_expression_pair> setItems;
};

} // namespace parser
} // namespace kuzu
