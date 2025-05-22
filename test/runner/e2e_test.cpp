#include <fstream>
#include <string>
#include <utility>

#include "common/assert.h"
#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "test_helper/test_helper.h"
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
        std::optional<uint64_t> bufferPoolSize, uint64_t checkpointWaitTimeout,
        const std::set<std::string>& connNames,
        std::vector<std::unique_ptr<TestStatement>> testStatements, std::string testPath)
        : datasetType{datasetType}, dataset{std::move(dataset)}, bufferPoolSize{bufferPoolSize},
          checkpointWaitTimeout{checkpointWaitTimeout}, testStatements{std::move(testStatements)},
          connNames{connNames}, testPath{std::move(testPath)} {}

    void SetUp() override {
        setUpDataset();
        BaseGraphTest::SetUp();
        if (bufferPoolSize) {
            systemConfig->bufferPoolSize = *bufferPoolSize;
        }
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
            // Determine the dataset root path. Uses `E2E_OVERRIDE_IMPORT_DIR` if set to test
            // datasets exported from earlier Kuzu versions, otherwise the default path.
            std::string rootDir = TestHelper::E2E_OVERRIDE_IMPORT_DIR.empty() ?
                                      "dataset/" :
                                      TestHelper::E2E_OVERRIDE_IMPORT_DIR;
            dataset = TestHelper::appendKuzuRootPath(rootDir + dataset);
        }
        }
    }

    void reWriteTests()
    {
        std::fstream file;
        std::string newFile;
        std::string currLine;
        file.open(testPath);

        const ::testing::TestInfo* const testInfo =
            ::testing::UnitTest::GetInstance()->current_test_info();

        // testStatements is NOT all tests specified in the file
        // Rather, it is all tests under the CASE 
        // testInfo->name()
        // We must find this case before we start replacing outputs
        // TODO <- This is inefficient (and likely not thread safe). 
        // Discuss an alternative implementation
        while(getline(file, currLine))
        {
            newFile += currLine + '\n';
            if (currLine == "-CASE " + std::string(testInfo->name()))
            {
                break;
            }
        }

        for (auto& statement : testStatements) {
            while (getline(file, currLine)) {
                if (!currLine.starts_with("-STATEMENT")) {
                    newFile += currLine + '\n';
                    continue;
                }

                newFile += currLine + '\n';
                std::string stmt = currLine;
                while (getline(file, currLine))
                {
                    if (currLine.starts_with("----"))
                    {
                        break;
                    }
                        newFile += currLine + '\n';
                        stmt += currLine;
                }
                
                // This lambda collaples multiple repeating space characters into a
                // single space character.
                // This was required since in parsing there were inexplicable
                // differences in whitespace causing erroneous errors
                // Since it is a small function and not needed else where it was
                // implemented as a lambda.
                // TODO BEFORE MERGING <- Check for an implementation of this
                // functionality in StringUtil


                auto normalize = [](const std::string& s) {
                    std::string result;
                    bool in_space = false;
                    for (char c : s) {
                        if (std::isspace(static_cast<unsigned char>(c))) {
                            if (!in_space) {
                                result += ' ';
                                in_space = true;
                            }
                        } else {
                            result += c;
                            in_space = false;
                        }
                    }
                    return result;
                };

                // THIS CHECK SHOULD NEVER WORK 
                if (normalize(stmt) != (normalize("-STATEMENT " + statement->query))) {
                    KU_UNREACHABLE;
                    newFile += currLine + '\n';
                    continue;
                }

                else {
                    switch (statement->testResultType) {
                        // Success results don't need anything after the dashes 
                        // -STATEMENT CREATE NODE TABLE  Person (ID INT64, PRIMARY KEY (ID)); 
                        // ---- ok
                        case ResultType::OK: {
                            newFile += statement->newOutput;
                        } break;
                        case ResultType::HASH: {
                            // Add result specifier
                            newFile += currLine + '\n';     
                            // Add produced hash
                            newFile += statement->newOutput; 
                            // Ignore expected hash
                            getline(file, currLine);         
                        } break;
                        // -CHECK_COLUMN_NAMES
                        // -STATEMENT MATCH (a:person) RETURN a.fName LIMIT 4
                        // ---- 5
                        // a.fName
                        // Alice
                        // Bob
                        // Carol
                        // Dan
                        case ResultType::TUPLES: {
                            try {
                                // We extract the number of expected tuples from the result
                                // specifier line and skip over as many tuples that
                                // were specified
                                size_t static constexpr numTuplesPrefix = std::string("---- ").size();
                                int count = std::stoi(currLine.substr(numTuplesPrefix)); 
                                for (int i = 0; i < count; ++i) {
                                    getline(file, currLine);
                                }
                                // Add the produced output, which contains the
                                // updated count of tuples
                                newFile += statement->newOutput; 
                            } catch (...) {
                                // Could not overwrite expected result
                                // error in parsing expected tuples
                                newFile += currLine + '\n';
                            }
                        } break;
                        case ResultType::CSV_FILE: // not supported yet
                        {
                            newFile += currLine + '\n';
                        }
                                break;
                        case ResultType::ERROR_MSG: {
                            newFile += statement->newOutput; // Add actual output (result and error msg)
                            int tmp = -1;
                            for (auto c : statement->newOutput) {
                                if (c == '\n') {
                                    tmp++;
                                }
                            }
                            for (int i = 0; i < tmp; ++i) { // TODO BEFORE MERGING <- This is wrong, and needs to change
                                getline(file, currLine); // Ignore produced error msg
                            }
                        } break;
                        case ResultType::ERROR_REGEX: {
                            newFile += statement->newOutput;
                            getline(file, currLine);
                            if (statement->newOutput != "ok\n")
                                newFile += currLine + '\n';
                        } break;
                    }
                    break; // goto next statement
                }
            }
        }

        while (getline(file, currLine)) // get any remaining lines in the file such as comments
        {
            newFile += currLine + '\n';
        }
        file.close();
        file.open(testPath, std::ios::trunc | std::ios::out);
        file << newFile;
    }

    void TearDown() override {
        DBTest::TearDown();
        removeIEDBPath();
        if (datasetType == TestGroup::DatasetType::CSV_TO_PARQUET ||
            datasetType == TestGroup::DatasetType::CSV_TO_JSON) {
            std::filesystem::remove_all(tempDatasetPath);
        }

        if (TestHelper::REWRITE_TESTS)
        {
            reWriteTests();
        }
    }

    void TestBody() override { runTest(testStatements, checkpointWaitTimeout, connNames); }
    std::string getInputDir() override { return dataset + "/"; }

private:
    TestGroup::DatasetType datasetType;
    std::string dataset;
    std::string tempDatasetPath;
    std::optional<uint64_t> bufferPoolSize;
    uint64_t checkpointWaitTimeout;
    std::vector<std::unique_ptr<TestStatement>> testStatements;
    std::set<std::string> connNames;
    std::string testPath;

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
                [path, datasetType, dataset, bufferPoolSize, checkpointWaitTimeout, connNames,
                    testStatements = std::move(testStatements)]() mutable -> DBTest* {
                    decltype(testStatements) testStatementsCopy;
                    for (const auto& testStatement : testStatements) {
                        testStatementsCopy.emplace_back(
                            std::make_unique<TestStatement>(*testStatement));
                    }
                    return new EndToEndTest(datasetType, dataset, bufferPoolSize,
                        checkpointWaitTimeout, connNames, std::move(testStatementsCopy), path);
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
        std::string import_data_dir;
        bool rewrite_tests = false;
        char* env_test_dir = std::getenv("E2E_TEST_FILES_DIRECTORY");
        char* env_import_data_dir = std::getenv("E2E_IMPORT_DB_DIR");
        char* env_rewrite_tests = std::getenv("E2E_REWRITE_TESTS");
        if (env_test_dir != nullptr) {
            test_dir = env_test_dir;
        } else {
            test_dir = "test/test_files";
        }

        if (env_import_data_dir != nullptr) {
            auto path =
                TestHelper::appendKuzuRootPath(std::filesystem::path(env_import_data_dir).string());
            if (!std::filesystem::exists(path)) {
                throw TestException("IMPORT DATABASE path does not exist: " + path);
            }
            import_data_dir = env_import_data_dir;
        }

        TestHelper::setE2ETestFilesDirectory(test_dir);
        TestHelper::setE2EImportDataDirectory(import_data_dir);

        if (env_rewrite_tests != nullptr && std::string(env_rewrite_tests) != "FALSE" &&
            std::string(env_rewrite_tests) != "OFF") {
            rewrite_tests = true;
        }
        TestHelper::setRewriteTests(rewrite_tests);

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
