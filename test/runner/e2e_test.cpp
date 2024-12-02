#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "test_runner/csv_converter.h"
#include "test_runner/test_parser.h"

using ::testing::Test;
using namespace kuzu::testing;
using namespace kuzu::common;

static void copyDir(const std::string& from, const std::string& to) {
    if (!std::filesystem::exists(from)) {
        throw TestException(stringFormat("Error copying nonexistent directory {}.", from));
    }
    if (std::filesystem::exists(to)) {
        throw TestException(
            stringFormat("Error copying directory {} to {}. {} already exists.", from, to, to));
    }
    std::error_code copyErrorCode;
    std::filesystem::copy(from, to, std::filesystem::copy_options::recursive, copyErrorCode);
    if (copyErrorCode) {
        throw TestException(stringFormat("Error copying directory {} to {}.  Error Message: {}",
            from, to, copyErrorCode.message()));
    }
}

class EndToEndTest : public DBTest {
public:
    explicit EndToEndTest(TestGroup::DatasetType datasetType, std::string dataset,
        uint64_t bufferPoolSize, uint64_t checkpointWaitTimeout,
        const std::set<std::string>& connNames,
        std::vector<std::unique_ptr<TestStatement>> testStatements)
        : datasetType{datasetType}, dataset{std::move(dataset)}, bufferPoolSize{bufferPoolSize},
          checkpointWaitTimeout{checkpointWaitTimeout}, testStatements{std::move(testStatements)},
          connNames{connNames} {}

    void SetUp() override {
        setUpDataset();
        BaseGraphTest::SetUp();
        systemConfig->bufferPoolSize = bufferPoolSize;
        bool generateBinaryDemo =
            !std::getenv("USE_EXISTING_BINARY_DATASET") && dataset.ends_with("binary-demo");
        if (datasetType == TestGroup::DatasetType::KUZU && dataset != "empty" &&
            !generateBinaryDemo) {
            copyDir(dataset, databasePath);
        }
        createDB(checkpointWaitTimeout);
        createConns(connNames);
        if (datasetType != TestGroup::DatasetType::KUZU && dataset != "empty") {
            initGraph();
        } else if (generateBinaryDemo) {
            initGraph(TestHelper::appendKuzuRootPath("dataset/demo-db/parquet/"));
        }
    }

    void setUpDataset() {
        switch (datasetType) {
        case TestGroup::DatasetType::CSV_TO_PARQUET: {
            auto csvDatasetPath = TestHelper::appendKuzuRootPath("dataset/" + dataset);
            tempDatasetPath = generateTempDatasetPath();
            CSVConverter converter(csvDatasetPath, tempDatasetPath, bufferPoolSize, ".parquet");
            converter.convertCSVDataset();
            dataset = tempDatasetPath;
        } break;
        case TestGroup::DatasetType::CSV_TO_JSON: {
            auto csvDatasetPath = TestHelper::appendKuzuRootPath("dataset/" + dataset);
            tempDatasetPath = generateTempDatasetPath();
            CSVConverter converter(csvDatasetPath, tempDatasetPath, bufferPoolSize, ".json");
            converter.convertCSVDataset();
            dataset = tempDatasetPath;
        } break;
        default: {
            dataset = TestHelper::appendKuzuRootPath("dataset/" + dataset);
        }
        }
    }

    void TearDown() override {
        DBTest::TearDown();
        removeIEDBPath();
        if (datasetType == TestGroup::DatasetType::CSV_TO_PARQUET ||
            datasetType == TestGroup::DatasetType::CSV_TO_JSON) {
            std::filesystem::remove_all(tempDatasetPath);
        }
    }

    void TestBody() override { runTest(testStatements, checkpointWaitTimeout, connNames); }
    std::string getInputDir() override { return dataset + "/"; }

private:
    TestGroup::DatasetType datasetType;
    std::string dataset;
    std::string tempDatasetPath;
    uint64_t bufferPoolSize;
    uint64_t checkpointWaitTimeout;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
    std::set<std::string> connNames;

    std::string generateTempDatasetPath() {
        std::string datasetName = dataset;
        std::replace(datasetName.begin(), datasetName.end(), '/', '_');
        return TestHelper::getTempDir(datasetName + "_parquet_" + getTestGroupAndName());
    }
};

void parseAndRegisterTestGroup(const std::string& path, bool generateTestList = false) {
    // Check for invalid characters in the file name (see ISSUE 4510)
    auto filename = std::filesystem::path(path).filename().string();
    if (filename.find('-') != std::string::npos) {
        throw TestException("Invalid test file name containing '-': " + filename);
    }
    auto testParser = std::make_unique<TestParser>(path);
    auto testGroup = testParser->parseTestFile();
    if (testGroup->isValid() && testGroup->hasStatements()) {
        auto datasetType = testGroup->datasetType;
        auto dataset = testGroup->dataset;
        auto testCases = std::move(testGroup->testCases);
        auto bufferPoolSize = testGroup->bufferPoolSize;
        auto checkpointWaitTimeout = testGroup->checkpointWaitTimeout;
        for (auto& [testCaseName, testStatements] : testCases) {
            // Check for invalid characters in the case name (see ISSUE 4510)
            if (testCaseName.find('-') != std::string::npos) {
                throw TestException("Invalid test case name containing '-': " + testCaseName);
            }
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
                    decltype(testStatements) testStatementsCopy;
                    for (const auto& testStatement : testStatements) {
                        testStatementsCopy.emplace_back(
                            std::make_unique<TestStatement>(*testStatement));
                    }
                    return new EndToEndTest(datasetType, dataset, bufferPoolSize,
                        checkpointWaitTimeout, connNames, std::move(testStatementsCopy));
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
        if (!entry.is_regular_file() || std::filesystem::path(entry).extension() != ".test")
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
            std::filesystem::remove_all(TestHelper::getTestListFile());
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
    try {
        // Main logic
        std::string test_dir;
        char* env_test_dir = std::getenv("E2E_TEST_FILES_DIRECTORY");
        if (env_test_dir != nullptr) {
            test_dir = env_test_dir;
        } else {
            test_dir = "test/test_files";
        }
        TestHelper::setE2ETestFilesDirectory(test_dir);

        checkGtestParams(argc, argv);
        testing::InitGoogleTest(&argc, argv);
        if (argc > 1) {
            auto path = TestHelper::appendKuzuRootPath(
                (std::filesystem::path(TestHelper::E2E_TEST_FILES_DIRECTORY) / argv[1]).string());
            if (!std::filesystem::exists(path)) {
                throw TestException("Test path does not exist [" + path + "].");
            }
            scanTestFiles(path);
        }

        return RUN_ALL_TESTS();
    } catch (const std::exception& ex) {
        std::cerr << "Error: " << ex.what() << std::endl;
        return EXIT_FAILURE;
    }
}
