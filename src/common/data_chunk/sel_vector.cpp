#include "common/data_chunk/sel_vector.h"

#include <numeric>

#include "common/constants.h"
#include "common/types/types.h"

namespace kuzu {
namespace common {

// NOLINTNEXTLINE(cert-err58-cpp): always evaluated at compile time, and even not it would not throw
const std::array<sel_t, DEFAULT_VECTOR_CAPACITY> SelectionVector::INCREMENTAL_SELECTED_POS =
    []() constexpr noexcept {
        std::array<sel_t, DEFAULT_VECTOR_CAPACITY> selectedPos{};
        std::iota(selectedPos.begin(), selectedPos.end(), 0);
        return selectedPos;
    }();
} // namespace common
} // namespace kuzu
