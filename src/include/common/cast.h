#pragma once

#include "common/assert.h"

namespace kuzu {
namespace common {

template<typename FROM, typename TO>
TO ku_dynamic_cast(FROM old) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    TO newVal = dynamic_cast<TO>(old);
    if constexpr (std::is_pointer<FROM>()) {
        KU_ASSERT(newVal != nullptr);
    }
    return newVal;
#else
    return reinterpret_cast<TO>(old);
#endif
}

} // namespace common
} // namespace kuzu
