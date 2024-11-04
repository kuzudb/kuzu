#include "function/aggregate/count.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

void CountFunction::updateAll(uint8_t* state_, ValueVector* input, uint64_t multiplicity,
    MemoryManager* /*memoryManager*/) {
    auto state = reinterpret_cast<CountState*>(state_);
    if (input->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < input->state->getSelVector().getSelSize(); ++i) {
            state->count += multiplicity;
        }
    } else {
        for (auto i = 0u; i < input->state->getSelVector().getSelSize(); ++i) {
            auto pos = input->state->getSelVector()[i];
            if (!input->isNull(pos)) {
                state->count += multiplicity;
            }
        }
    }
}

void CountFunction::paramRewriteFunc(binder::expression_vector& arguments) {
    KU_ASSERT(arguments.size() == 1);
    if (ExpressionUtil::isNodePattern(*arguments[0])) {
        auto node = (NodeExpression*)arguments[0].get();
        arguments[0] = node->getInternalID();
    } else if (ExpressionUtil::isRelPattern(*arguments[0])) {
        auto rel = (RelExpression*)arguments[0].get();
        arguments[0] = rel->getInternalIDProperty();
    }
}

function_set CountFunction::getFunctionSet() {
    function_set result;
    for (auto& type : LogicalTypeUtils::getAllValidLogicTypeIDs()) {
        for (auto isDistinct : std::vector<bool>{true, false}) {
            result.push_back(AggregateFunctionUtils::getAggFunc<CountFunction>(name, type,
                LogicalTypeID::INT64, isDistinct, paramRewriteFunc));
        }
    }
    return result;
}

} // namespace function
} // namespace kuzu
