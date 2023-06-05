#include "graph_test/graph_test.h"
#include "test_runner/test_parser.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndTest : public DBTest {
public:
    explicit EndToEndTest(std::string dataset, uint64_t bufferPoolSize,
        std::vector<std::unique_ptr<TestStatement>> testStatements)
        : dataset{dataset}, bufferPoolSize{bufferPoolSize}, testStatements{
                                                                std::move(testStatements)} {}

    void SetUp() override {
        BaseGraphTest::SetUp();
        systemConfig->bufferPoolSize = bufferPoolSize;
        createDBAndConn();
        initGraph();
    }

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/" + dataset + "/");
    }
    void TestBody() override { runTest(testStatements); }

private:
    std::string dataset;
    uint64_t bufferPoolSize;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
};

void parseAndRegisterTestGroup(const std::string& path) {
    auto testParser = std::make_unique<TestParser>(path);
    auto testGroup = std::move(testParser->parseTestFile());
    if (testGroup->isValid() && testGroup->hasStatements()) {
        auto dataset = testGroup->dataset;
        auto testCases = std::move(testGroup->testCases);
        auto bufferPoolSize = testGroup->bufferPoolSize;
        for (auto& [testCaseName, testStatements] : testCases) {
            testing::RegisterTest(testGroup->group.c_str(), testCaseName.c_str(), nullptr, nullptr,
                __FILE__, __LINE__,
                [dataset, bufferPoolSize,
                    testStatements = std::move(testStatements)]() mutable -> DBTest* {
                    return new EndToEndTest(dataset, bufferPoolSize, std::move(testStatements));
                });
        }
    } else {
        throw TestException("Invalid test file [" + path + "].");
    }
}

void scanTestFiles(const std::string& path) {
    if (std::filesystem::is_regular_file(path)) {
        parseAndRegisterTestGroup(path);
        return;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (!entry.is_regular_file() || FileUtils::getFileExtension(entry) != ".test")
            continue;
        parseAndRegisterTestGroup(entry.path().string());
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    std::string path = TestHelper::E2E_TEST_FILES_DIRECTORY;
    if (argc > 1) {
        path = path + "/" + argv[1];
    }
    path = TestHelper::appendKuzuRootPath(path);
    if (!FileUtils::fileOrPathExists(path)) {
        throw TestException("Test path not exists [" + path + "].");
    }
    scanTestFiles(path);
    return RUN_ALL_TESTS();
}
