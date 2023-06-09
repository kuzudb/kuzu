#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "test_runner/csv_to_parquet_converter.h"
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

    void TearDown() override {
        FileUtils::removeDir(TestHelper::appendKuzuRootPath(TestHelper::PARQUET_TEMP_DATASET_PATH));
        FileUtils::removeDir(TestHelper::getTmpTestDir());
    }

    std::string getInputDir() override { return dataset + "/"; }

    void TestBody() override { runTest(testStatements); }

private:
    std::string dataset;
    uint64_t bufferPoolSize;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
};

void parseAndRegisterTestGroup(const std::string& path, bool generateTestList = false) {
    auto testParser = std::make_unique<TestParser>(path);
    auto testGroup = std::move(testParser->parseTestFile());
    if (testGroup->isValid() && testGroup->hasStatements()) {
        auto dataset = testGroup->dataset;
        auto testCases = std::move(testGroup->testCases);
        auto bufferPoolSize = testGroup->bufferPoolSize;
        if (testGroup->datasetType == TestGroup::DatasetType::CSV_TO_PARQUET) {
            CSVToParquetConverter::convertCSVDatasetToParquet(dataset);
        } else {
            dataset = TestHelper::appendKuzuRootPath("dataset/" + dataset);
        }
        for (auto& [testCaseName, testStatements] : testCases) {
            if (generateTestList) {
                std::ofstream testList(TestHelper::getTestListFile(), std::ios_base::app);
                testList << testGroup->group + "." + testCaseName + " " + path + "\n";
            }
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
    std::string testListFile = TestHelper::appendKuzuRootPath(
        FileUtils::joinPath(TestHelper::E2E_TEST_FILES_DIRECTORY, "test_list"));
    FileUtils::removeFileIfExists(testListFile);
    FileUtils::createDirIfNotExists(
        TestHelper::appendKuzuRootPath(TestHelper::PARQUET_TEMP_DATASET_PATH));
    if (std::filesystem::is_regular_file(path)) {
        parseAndRegisterTestGroup(path);
        return;
    }
    for (const auto& entry : std::filesystem::recursive_directory_iterator(path)) {
        if (!entry.is_regular_file() || FileUtils::getFileExtension(entry) != ".test")
            continue;
        parseAndRegisterTestGroup(entry.path().string(), true);
    }
}

std::string findTestFile(std::string testCase) {
    std::ifstream infile(TestHelper::getTestListFile());
    std::string line;
    while (std::getline(infile, line)) {
        std::vector token = StringUtils::splitBySpace(line);
        if (token[0] == testCase) {
            return token[1];
        }
    }
    return "";
}

void checkCtestParams(int argc, char** argv) {
    if (argc > 1) {
        std::string argument = argv[1];
        bool runFromCTest = false;
        if (argument == "--gtest_list_tests") {
            scanTestFiles(TestHelper::appendKuzuRootPath(TestHelper::E2E_TEST_FILES_DIRECTORY));
        }
        if (argument.starts_with("--gtest_filter=")) {
            FileUtils::createDirIfNotExists(
                TestHelper::appendKuzuRootPath(TestHelper::PARQUET_TEMP_DATASET_PATH));
            std::string testCaseFile = findTestFile(argument.substr(15));
            if (testCaseFile.empty()) {
                scanTestFiles(TestHelper::appendKuzuRootPath(TestHelper::E2E_TEST_FILES_DIRECTORY));
            } else {
                parseAndRegisterTestGroup(testCaseFile);
            }
        }
    }
}

int main(int argc, char** argv) {
    checkCtestParams(argc, argv);
    testing::InitGoogleTest(&argc, argv);
    if (argc > 1) {
        auto path = TestHelper::appendKuzuRootPath(
            FileUtils::joinPath(TestHelper::E2E_TEST_FILES_DIRECTORY, argv[1]));
        if (!FileUtils::fileOrPathExists(path)) {
            throw TestException("Test path not exists [" + path + "].");
        }
        scanTestFiles(path);
    }
    return RUN_ALL_TESTS();
}
