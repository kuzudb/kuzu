#include "main/kuzu.h"

using namespace kuzu::main;

namespace kuzu {
namespace testing {

struct TestCommand {
    std::string name;
    std::string query;
    uint64_t numThreads = 4;
    std::string encodedJoin;
    uint64_t expectedNumTuples = 0;
    bool expectedError = false;
    bool expectedOk = false;
    std::vector<std::string> expectedTuples;
    bool enumerate = false;
    bool checkOutputOrder = false;
};

class TestCase {
public:
    std::string group;
    std::string name;
    std::string dataset;
    std::vector<std::unique_ptr<TestCommand>> commands;

    bool isValid() const {
        return !group.empty() && !name.empty() && !dataset.empty() && !commands.empty();
    }

    void parseTestFile(const std::string& path);
};
     

} // namespace testing
} // namespace kuzu
