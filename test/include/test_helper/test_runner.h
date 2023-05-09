#include "main/kuzu.h"
#include "test_helper/test_case.h" // CHECK

using namespace kuzu::main;

namespace kuzu {
namespace testing {

class TestRunner {
public:
    static void runTest(const std::vector<std::unique_ptr<TestCommand>>& commands, Connection& conn);
};
     

} // namespace testing
} // namespace kuzu
