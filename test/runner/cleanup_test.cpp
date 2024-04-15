#include "test_helper/test_helper.h"

using namespace kuzu::testing;
using namespace kuzu::common;

int main(int argc, char** argv) {
    if (argc > 1 && std::string(argv[1]) == "--gtest_list_tests") {
        std::filesystem::remove_all(TestHelper::getTempDir());
    }
    return 0;
}
