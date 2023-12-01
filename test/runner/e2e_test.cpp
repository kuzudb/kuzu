#include "common/exception/not_implemented.h"
#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "test_runner/csv_to_parquet_converter.h"
#include "test_runner/test_parser.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

class EndToEndTest : public DBTest {
public:
    explicit EndToEndTest(TestGroup::DatasetType datasetType, std::string dataset,
        uint64_t bufferPoolSize, uint64_t checkpointWaitTimeout,
        const std::set<std::string>& connNames,
        std::vector<std::unique_ptr<TestStatement>> testStatements)
        : datasetType{datasetType}, dataset{std::move(dataset)}, bufferPoolSize{bufferPoolSize},
          checkpointWaitTimeout{checkpointWaitTimeout}, connNames{connNames},
          testStatements{std::move(testStatements)} {}

    void SetUp() override {
        setUpDataset();
        BaseGraphTest::SetUp();
        systemConfig->bufferPoolSize = bufferPoolSize;
        createDB(checkpointWaitTimeout);
        createConns(connNames);
        if (dataset != "empty") {
            initGraph();
        }
    }

    void setUpDataset() {
        parquetTempDatasetPath = generateParquetTempDatasetPath();
        dataset = TestHelper::appendKuzuRootPath("dataset/" + dataset);
        if (datasetType == TestGroup::DatasetType::CSV_TO_PARQUET) {
            throw NotImplementedException("CSV_TO_PARQUET dataset type is not implemented yet.");
        }
    }

    void TearDown() override {
        FileUtils::removeDir(databasePath);
        FileUtils::removeDir(parquetTempDatasetPath);
    }

    void TestBody() override { runTest(testStatements, checkpointWaitTimeout, connNames); }
    std::string getInputDir() override { return dataset + "/"; }

private:
    TestGroup::DatasetType datasetType;
    std::string dataset;
    std::string parquetTempDatasetPath;
    uint64_t bufferPoolSize;
    uint64_t checkpointWaitTimeout;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
    std::set<std::string> connNames;

    std::string generateParquetTempDatasetPath() {
        return TestHelper::appendKuzuRootPath(
            TestHelper::PARQUET_TEMP_DATASET_PATH +
            CSVToParquetConverter::replaceSlashesWithUnderscores(dataset) + getTestGroupAndName() +
            TestHelper::getMillisecondsSuffix());
    }
};

void parseAndRegisterTestGroup(const std::string& path, bool generateTestList = false) {
    auto testParser = std::make_unique<TestParser>(path);
    auto testGroup = std::move(testParser->parseTestFile());
    if (testGroup->isValid() && testGroup->hasStatements()) {
        auto datasetType = testGroup->datasetType;
        auto dataset = testGroup->dataset;
        auto testCases = std::move(testGroup->testCases);
        auto bufferPoolSize = testGroup->bufferPoolSize;
        auto checkpointWaitTimeout = testGroup->checkpointWaitTimeout;
        for (auto& [testCaseName, testStatements] : testCases) {
            if (generateTestList) {
                std::ofstream testList(TestHelper::getTestListFile(), std::ios_base::app);
                testList << testGroup->group + "." + testCaseName + " " + path + "\n";
            }
            if (empty(testCaseName)) {
                throw TestException("Missing test case name (-CASE) [" + path + "].");
            }
            auto connNames = testGroup->testCasesConnNames[testCaseName];
            testing::RegisterTest(testGroup->group.c_str(), testCaseName.c_str(), nullptr, nullptr,
                __FILE__, __LINE__,
                [datasetType, dataset, bufferPoolSize, checkpointWaitTimeout, connNames,
                    testStatements = std::move(testStatements)]() mutable -> DBTest* {
                    return new EndToEndTest(datasetType, dataset, bufferPoolSize,
                        checkpointWaitTimeout, connNames, std::move(testStatements));
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
        parseAndRegisterTestGroup(entry.path().string(), true);
    }
}

std::string findTestFile(const std::string& testCase) {
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

void checkGtestParams(int argc, char** argv) {
    if (argc > 1) {
        std::string argument = argv[1];
        if (argument == "--gtest_list_tests") {
            FileUtils::removeFileIfExists(TestHelper::getTestListFile());
            scanTestFiles(TestHelper::appendKuzuRootPath(TestHelper::E2E_TEST_FILES_DIRECTORY));
        }
        if (argument.starts_with("--gtest_filter=")) {
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
    checkGtestParams(argc, argv);
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
