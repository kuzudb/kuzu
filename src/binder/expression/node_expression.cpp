#include "binder/expression/node_expression.h"

#include "binder/expression/property_expression.h"

namespace kuzu {
namespace binder {

std::shared_ptr<Expression> NodeExpression::getPrimaryKey(common::table_id_t tableID) const {
    for (auto& e : propertyExprs) {
        if (e->constCast<PropertyExpression>().isPrimaryKey(tableID)) {
            return e->copy();
        }
    }
    KU_UNREACHABLE;
}

} // namespace binder
} // namespace kuzu
