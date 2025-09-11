#pragma once

#include <cstdint>

namespace kuzu {
namespace main {
class Connection;
} // namespace main

namespace testing {

struct FSMLeakChecker {
    // Performs the whole leak check sequence; throws/asserts on failure
    static void checkForLeakedPages(main::Connection* conn);
};

} // namespace testing
} // namespace kuzu
