#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;

class RunDynamicTest : public DBTest {
public:
    std::string getInputDir() override { return TestHelper::appendKuzuRootPath(dataset_); }

    explicit RunDynamicTest(std::vector<std::string> testFiles, std::string dataset)
        : testFiles_(testFiles), dataset_(dataset) {}

    void TestBody() override {
        for (auto& file : testFiles_) {
            runTest(file);
        }
    }

private:
    std::vector<std::string> testFiles_;
    std::string dataset_;
};

void RegisterE2ETests(const std::vector<TestConfig> testConfig) {
    for (auto testItem : testConfig) {
        testing::RegisterTest(testItem.testSuiteName.c_str(), testItem.testName.c_str(), nullptr,
            testItem.testName.c_str(), __FILE__, __LINE__,
            [=]() -> DBTest* { return new RunDynamicTest(testItem.files, testItem.dataset); });
    }
}

std::vector<TestConfig> scanEndToEndTestFiles(const std::string& path) {
    TestConfig testConfig;
    std::vector<TestConfig> tests;

    for (const auto& entry : std::filesystem::directory_iterator(path)) {
        std::string dirName = entry.path().filename().string();
        testConfig.testSuiteName = TestHelper::convertSnakeCaseToCamelCase(dirName + "Test");
        std::replace(dirName.begin(), dirName.end(), '_', '-');
        testConfig.dataset = "dataset/" + dirName + "/";

        for (const auto& subEntry :
            std::filesystem::directory_iterator(path + "/" + entry.path().filename().string())) {
            testConfig.files.clear();
            if (subEntry.is_directory()) {
                testConfig.testName =
                    TestHelper::convertSnakeCaseToCamelCase(subEntry.path().filename().string());
                for (const auto& file : std::filesystem::directory_iterator(subEntry.path())) {
                    if (file.is_regular_file()) {
                        testConfig.files.push_back(file.path().string());
                    }
                }
            } else {
                testConfig.testName = testConfig.testSuiteName;
                testConfig.files.push_back(subEntry.path().string());
            }
            tests.push_back(testConfig);
        }
    }

    return tests;
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    std::vector<TestConfig> tests =
        scanEndToEndTestFiles(TestHelper::appendKuzuRootPath("test/test_files/e2e"));

    RegisterE2ETests(tests);
    return RUN_ALL_TESTS();
}
