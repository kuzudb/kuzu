#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndReadTest : public DBTest {
public:
    EndToEndReadTest(TestConfig testConfig) : testConfig(std::move(testConfig)) {}
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/" + testConfig.dataset + "/");
    }
    void TestBody() override {
        for (auto& file : testConfig.files) {
            if (testConfig.checkOrder) {
                runTestAndCheckOrder(file);
            } else {
                runTest(file);
            }
        }
    }

private:
    TestConfig testConfig;
};

void registerTests(const std::vector<TestConfig> testConfig) {
    for (auto testItem : testConfig) {
        testing::RegisterTest(testItem.testGroup.c_str(), testItem.testName.c_str(), nullptr,
            nullptr, __FILE__, __LINE__,
            [=]() -> DBTest* { return new EndToEndReadTest(testItem); });
    }
}

TestConfig parseTestGroup(const std::string& path) {
    auto testConfig = TestHelper::parseGroupFile(path + "/test.group");
    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        if (entry.is_regular_file() && FileUtils::getFileExtension(entry) == ".test") {
            testConfig.files.push_back(entry.path().string());
        }
    }
    return testConfig;
}

void scanTestFiles(const std::string& path, std::vector<TestConfig>& configs) {
    for (const auto& directory : FileUtils::findAllDirectories(path)) {
        if (FileUtils::fileOrPathExists(directory + "/test.group")) {
            TestConfig config = parseTestGroup(directory);
            configs.push_back(config);
        }
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    std::vector<TestConfig> configs;
    scanTestFiles(TestHelper::appendKuzuRootPath("test/test_files"), configs);
    registerTests(configs);
    return RUN_ALL_TESTS();
}
