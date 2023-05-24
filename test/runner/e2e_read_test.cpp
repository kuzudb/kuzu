#include "graph_test/graph_test.h"
#include "test_runner/test_parser.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndReadTest : public DBTest {
public:
    explicit EndToEndReadTest(
        std::string dataset, std::vector<std::unique_ptr<TestStatement>> testStatements)
        : dataset{dataset}, testStatements{std::move(testStatements)} {}

    std::string getInputDir() override {
        return TestHelper::appendKuzuRootPath("dataset/" + dataset + "/");
    }
    void TestBody() override { runTest(testStatements); }

private:
    std::string dataset;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
};

void parseAndRegisterTestGroup(const std::string& path) {
    auto testParser = std::make_unique<TestParser>();
    auto testGroup = std::move(testParser->parseTestFile(path));
    if (testGroup->isValid() && testGroup->hasStatements()) {
        auto dataset = testGroup->dataset;
        auto testCases = std::move(testGroup->testCases);
        for (auto& [testCaseName, testStatements] : testCases) {
            testing::RegisterTest(testGroup->group.c_str(), testCaseName.c_str(), nullptr, nullptr,
                __FILE__, __LINE__,
                [dataset, testStatements = std::move(testStatements)]() mutable -> DBTest* {
                    return new EndToEndReadTest(dataset, std::move(testStatements));
                });
        }
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
    std::string path = "test/test_files";
    if (argc > 1) {
        path = argv[1];
    }
    path = TestHelper::appendKuzuRootPath(path);
    if (!FileUtils::fileOrPathExists(path)) {
        throw Exception("Test directory not exists! [" + path + "].");
    }
    scanTestFiles(path);
    return RUN_ALL_TESTS();
}
