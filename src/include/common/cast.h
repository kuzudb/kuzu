#pragma once

namespace kuzu {
namespace common {

template<typename FROM, typename TO>
TO ku_dynamic_cast(FROM old) {
#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
    return dynamic_cast<TO>(old);
#else
    return reinterpret_cast<TO>(old);
#endif
}

} // namespace common
} // namespace kuzu
