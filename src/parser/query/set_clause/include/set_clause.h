#pragma once

#include "src/parser/expression/include/parsed_expression.h"

namespace graphflow {
namespace parser {

struct SetItem {

    SetItem(unique_ptr<ParsedExpression> origin, unique_ptr<ParsedExpression> target)
        : origin{move(origin)}, target{move(target)} {}

    unique_ptr<ParsedExpression> origin;
    unique_ptr<ParsedExpression> target;
};

class SetClause {

public:
    SetClause() = default;
    ~SetClause() = default;

    inline void addSetItem(unique_ptr<SetItem> setItem) { setItems.push_back(move(setItem)); }

    inline uint32_t getNumSetItems() const { return setItems.size(); }

    inline SetItem* getSetItem(uint32_t idx) const { return setItems[idx].get(); }

private:
    vector<unique_ptr<SetItem>> setItems;
};

} // namespace parser
} // namespace graphflow
