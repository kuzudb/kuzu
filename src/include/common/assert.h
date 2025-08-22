#pragma once

#include <random>

#include "common/exception/buggify.h"
#include "common/exception/internal.h"
#include "common/string_format.h"

namespace kuzu {
namespace common {

[[noreturn]] inline void kuAssertFailureInternal(const char* condition_name, const char* file,
    int linenr) {
    // LCOV_EXCL_START
    throw InternalException(stringFormat("Assertion failed in file \"{}\" on line {}: {}", file,
        linenr, condition_name));
    // LCOV_EXCL_STOP
}

[[noreturn]] inline void kuAssertBuggify(const char* condition_name, const char* file, int linenr) {
    // LCOV_EXCL_START
    throw BuggifyException(stringFormat("Assertion failed in file \"{}\" on line {}: {}", file,
        linenr, condition_name));
    // LCOV_EXCL_STOP
}

inline thread_local bool buggifyEnabled = false;

class BuggifyScope {
public:
    BuggifyScope() { buggifyEnabled = true; }
    ~BuggifyScope() { buggifyEnabled = false; }
};

inline bool shouldBuggify() {
#ifdef KUZU_BUGGIFY
    if (!buggifyEnabled) {
        return false;
    }
    static thread_local std::mt19937 gen = [] {
        const char* seed_env = std::getenv("BUGGIFY_SEED");
        uint32_t seed = seed_env ? std::atoi(seed_env) : std::random_device{}();
        return std::mt19937(seed);
    }();
    static thread_local std::uniform_real_distribution dis(0.0f, 1.0f);
    return dis(gen) < 0.5f;
#endif
    return false;
}

#define KU_ASSERT_UNCONDITIONAL(condition)                                                         \
    kuzu::common::shouldBuggify() ?                                                                \
        kuzu::common::kuAssertBuggify(#condition, __FILE__, __LINE__) :                            \
    static_cast<bool>(condition) ?                                                                 \
        void(0) :                                                                                  \
        kuzu::common::kuAssertFailureInternal(#condition, __FILE__, __LINE__)

#if defined(KUZU_RUNTIME_CHECKS) || !defined(NDEBUG)
#define RUNTIME_CHECK(code) code
#define KU_ASSERT(condition) KU_ASSERT_UNCONDITIONAL(condition)
#else
#define KU_ASSERT(condition) void(0)
#define RUNTIME_CHECK(code) void(0)
#endif

#define KU_UNREACHABLE                                                                             \
    /* LCOV_EXCL_START */ [[unlikely]] kuzu::common::kuAssertFailureInternal("KU_UNREACHABLE",     \
        __FILE__, __LINE__) /* LCOV_EXCL_STOP */
#define KU_UNUSED(expr) (void)(expr)

} // namespace common
} // namespace kuzu
