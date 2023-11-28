#include "processor/operator/fill_table_id.h"

#include "common/types/internal_id_t.h"
#include "processor/execution_context.h"

namespace kuzu {
namespace processor {

bool FillTableID::getNextTuplesInternal(ExecutionContext* context) {
    if (!children[0]->getNextTuple(context)) {
        return false;
    }
    auto data = (common::internalID_t*)internalIDVector->getData();
    for (auto i = 0u; i < internalIDVector->state->selVector->selectedSize; ++i) {
        data[i].tableID = tableID;
        auto a = data[i];
    }
    return true;
}

} // namespace processor
} // namespace kuzu
