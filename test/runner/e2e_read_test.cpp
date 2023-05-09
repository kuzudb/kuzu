#include "graph_test/graph_test.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndReadTest : public DBTest {
public:
    EndToEndReadTest(std::string dataset, std::vector<TestCommand> commands) : testCase(dataset, commands) {}

    std::string getInputDir() override {
        std::cout << "dataset: " << testCase->dataset << std::endl;
        return TestHelper::appendKuzuRootPath("dataset/" + testCase->dataset + "/");
    }
    void TestBody() override {
        runTest(testCase->commands);
    }

private:
    std::unique_ptr<TestCase> testCase;
    std::string dataset;
    std::vector<TestCommand> commands;
};

void scanTestFiles(const std::string& path) {
    // TODO: check if it is a single file or directory
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (!entry.is_regular_file() || FileUtils::getFileExtension(entry) != ".test") 
            continue;

        auto testCase = std::make_unique<TestCase>();
        testCase->parseTestFile(entry.path().string());

        if (testCase->isValid()) {
            for (auto& command : testCase->commands) {
                std::cout << command->name << " : " << command->query << " | "  << command->expectedNumTuples << std::endl;
//                for(auto& result : command->expectedTuples) {
//                    std::cout << result << std::endl;
//                }
            }

            std::cout << "DATASET::" << testCase->dataset << std::endl;

            testing::RegisterTest(testCase->group.c_str(), testCase->name.c_str(), nullptr,
                    nullptr, __FILE__, __LINE__,
                        [testCase = std::move(testCase)]() -> DBTest* { return new EndToEndReadTest(std::move(testCase)) });
       //               [&]() -> DBTest* { return new EndToEndReadTest(std::move(testCase)); });


        }

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
