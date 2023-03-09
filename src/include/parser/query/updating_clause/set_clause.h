#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

class SetClause : public UpdatingClause {
public:
    SetClause() : UpdatingClause{common::ClauseType::SET} {};
    ~SetClause() override = default;

    inline void addSetItem(
        std::pair<std::unique_ptr<ParsedExpression>, std::unique_ptr<ParsedExpression>> setItem) {
        setItems.push_back(std::move(setItem));
    }
    inline uint32_t getNumSetItems() const { return setItems.size(); }
    inline std::pair<ParsedExpression*, ParsedExpression*> getSetItem(uint32_t idx) const {
        return std::make_pair(setItems[idx].first.get(), setItems[idx].second.get());
    }

private:
    std::vector<std::pair<std::unique_ptr<ParsedExpression>, std::unique_ptr<ParsedExpression>>>
        setItems;
};

} // namespace parser
} // namespace kuzu
