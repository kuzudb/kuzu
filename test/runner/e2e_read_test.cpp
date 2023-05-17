#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndReadTest : public DBTest {
public:
    explicit EndToEndReadTest(
        std::string dataset, std::vector<std::unique_ptr<TestStatement>> statements)
        : dataset{std::move(dataset)}, statements{std::move(statements)} {}

    std::string getInputDir() override {
        std::cout << "dataset: " << dataset << std::endl;
        return TestHelper::appendKuzuRootPath("dataset/" + dataset + "/");
    }
    void TestBody() override { runTest(statements); }

private:
    std::string dataset;
    std::vector<std::unique_ptr<TestStatement>> statements;
};

void parseAndRegisterTestCase(const std::string& path) {
    auto testCase = std::make_unique<TestCase>();
    testCase->parseTestFile(path);
    if (testCase->isValid()) {
        auto dataset = testCase->dataset;
        auto statements = std::move(testCase->statements);
        testing::RegisterTest(testCase->group.c_str(), testCase->name.c_str(), nullptr, nullptr,
            __FILE__, __LINE__,
            [dataset = std::move(dataset), statements = std::move(statements)]() mutable
            -> DBTest* { return new EndToEndReadTest(std::move(dataset), std::move(statements)); });
    }
}

void scanTestFiles(const std::string& path) {
    if (std::filesystem::is_regular_file(path)) {
        parseAndRegisterTestCase(path);
        return;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (!entry.is_regular_file() || FileUtils::getFileExtension(entry) != ".test")
            continue;
        parseAndRegisterTestCase(entry.path().string());
    }
}

int main(int argc, char** argv) {
    testing::InitGoogleTest(&argc, argv);
    std::string path = "test/test_files/order_by";
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
