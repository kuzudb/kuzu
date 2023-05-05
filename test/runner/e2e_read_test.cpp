#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndReadTest : public DBTest {
public:
    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/" + testConfig.dataset + "/");
    }
    EndToEndReadTest(TestConfig testConfig) : testConfig(std::move(testConfig)) {}
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

std::vector<TestConfig> scanTestFiles(const std::string& path) {
    TestConfig testConfig;
    std::vector<TestConfig> tests;
    std::string previousDirectory;
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (!entry.is_regular_file())
            continue;
        std::string testGroupFile = FileUtils::getParentPath(entry) + "/test.group";
        if (!FileUtils::fileOrPathExists(testGroupFile))
            continue;
        if (FileUtils::getParentPathStem(entry) != previousDirectory) {
            if (testConfig.isValid())
                tests.push_back(testConfig);
            testConfig = TestHelper::parseGroupFile(testGroupFile);
        }
        if (FileUtils::getFileExtension(entry) == ".test") {
            testConfig.files.push_back(entry.path().string());
        }
        previousDirectory = FileUtils::getParentPathStem(entry);
    }
    if (testConfig.isValid())
        tests.push_back(testConfig);
    return tests;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    std::vector<TestConfig> tests =
        scanTestFiles(TestHelper::appendKuzuRootPath("test/test_files"));
    registerTests(tests);
    return RUN_ALL_TESTS();
}
