#include "function/aggregate/count.h"

#include "binder/expression/expression_util.h"
#include "binder/expression/node_expression.h"
#include "binder/expression/rel_expression.h"

using namespace kuzu::common;
using namespace kuzu::storage;
using namespace kuzu::binder;

namespace kuzu {
namespace function {

void CountFunction::updateAll(
    uint8_t* state_, ValueVector* input, uint64_t multiplicity, MemoryManager* /*memoryManager*/) {
    auto state = reinterpret_cast<CountState*>(state_);
    if (input->hasNoNullsGuarantee()) {
        for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
            state->count += multiplicity;
        }
    } else {
        for (auto i = 0u; i < input->state->selVector->selectedSize; ++i) {
            auto pos = input->state->selVector->selectedPositions[i];
            if (!input->isNull(pos)) {
                state->count += multiplicity;
            }
        }
    }
}

void CountFunction::paramRewriteFunc(binder::expression_vector& arguments) {
    assert(arguments.size() == 1);
    if (ExpressionUtil::isNodeVariable(*arguments[0])) {
        auto node = (NodeExpression*)arguments[0].get();
        arguments[0] = node->getInternalID();
    } else if (ExpressionUtil::isRelVariable(*arguments[0])) {
        auto rel = (RelExpression*)arguments[0].get();
        arguments[0] = rel->getInternalIDProperty();
    }
}

} // namespace function
} // namespace kuzu
