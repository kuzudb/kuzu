#pragma once

#include "parser/expression/parsed_expression.h"
#include "updating_clause.h"

namespace kuzu {
namespace parser {

struct SetItem {

    SetItem(unique_ptr<ParsedExpression> origin, unique_ptr<ParsedExpression> target)
        : origin{move(origin)}, target{move(target)} {}

    unique_ptr<ParsedExpression> origin;
    unique_ptr<ParsedExpression> target;
};

class SetClause : public UpdatingClause {

public:
    SetClause() : UpdatingClause{ClauseType::SET} {};
    ~SetClause() override = default;

    inline void addSetItem(unique_ptr<SetItem> setItem) { setItems.push_back(move(setItem)); }
    inline uint32_t getNumSetItems() const { return setItems.size(); }
    inline SetItem* getSetItem(uint32_t idx) const { return setItems[idx].get(); }

private:
    vector<unique_ptr<SetItem>> setItems;
};

} // namespace parser
} // namespace kuzu
