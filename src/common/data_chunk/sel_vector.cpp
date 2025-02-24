#include "common/data_chunk/sel_vector.h"

#include <array>
#include <numeric>

#include "common/system_config.h"
#include "common/types/types.h"
#include "common/vector/value_vector.h"

namespace kuzu {
namespace common {

// NOLINTNEXTLINE(cert-err58-cpp): always evaluated at compile time, and even not it would not throw
static const std::array<sel_t, DEFAULT_VECTOR_CAPACITY> INCREMENTAL_SELECTED_POS =
    []() constexpr noexcept {
        std::array<sel_t, DEFAULT_VECTOR_CAPACITY> selectedPos{};
        std::iota(selectedPos.begin(), selectedPos.end(), 0);
        return selectedPos;
    }();

SelectionVector::SelectionVector() : SelectionVector{DEFAULT_VECTOR_CAPACITY} {}

void SelectionVector::setToUnfiltered() {
    selectedPositions = const_cast<sel_t*>(INCREMENTAL_SELECTED_POS.data());
    state = State::STATIC;
}
void SelectionVector::setToUnfiltered(sel_t size) {
    KU_ASSERT(size <= capacity);
    selectedPositions = const_cast<sel_t*>(INCREMENTAL_SELECTED_POS.data());
    selectedSize = size;
    state = State::STATIC;
}

std::vector<SelectionVector*> SelectionVector::constructFromValueVectors(
    const std::vector<std::shared_ptr<common::ValueVector>>& vec) {
    std::vector<SelectionVector*> ret;
    std::transform(vec.begin(), vec.end(), std::back_inserter(ret),
        [](const auto& vec) -> SelectionVector* { return vec->getSelVectorPtr(); });
    return ret;
}

} // namespace common
} // namespace kuzu
