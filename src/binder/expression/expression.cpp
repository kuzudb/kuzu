#include "binder/expression/expression.h"

using namespace kuzu::common;

namespace kuzu {
namespace binder {

expression_vector Expression::splitOnAND() {
    expression_vector result;
    if (ExpressionType::AND == expressionType) {
        for (auto& child : children) {
            for (auto& exp : child->splitOnAND()) {
                result.push_back(exp);
            }
        }
    } else {
        result.push_back(shared_from_this());
    }
    return result;
}

} // namespace binder
} // namespace kuzu
