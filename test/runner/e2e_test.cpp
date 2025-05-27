#include <fstream>
#include <string>
#include <string_view>
#include <utility>

#include "common/string_utils.h"
#include "graph_test/graph_test.h"
#include "spdlog/spdlog.h"
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

    void TearDown() override {
        DBTest::TearDown();
        removeIEDBPath();
        if (datasetType == TestGroup::DatasetType::CSV_TO_PARQUET ||
            datasetType == TestGroup::DatasetType::CSV_TO_JSON) {
            std::filesystem::remove_all(tempDatasetPath);
        }

        if (TestHelper::REWRITE_TESTS) {
            rewriteTestFile();
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

    // Used when `REWRITE_TESTS` mode is enabled.
    //
    // Currently, the implementation of the rewrite functionality is inefficient.
    // 1) The testStatements vector container does not contain all the tests specified in the file.
    //    Rather, it only contains tests for a single CASE. See `parseAndRegisterFileGroup()`.
    //    When rewriting the output, we need to first find this CASE.
    // 2) This function will be called from `TearDown` each time a CASE has completed running, which
    //    implies multiple calls for test files that have more than one CASE block, possibly in
    //    parallel. The current code does not handle calls for the same test file in parallel, and
    //    rewrite mode is expected to be run in single-threaded mode using `TEST_JOBS=1`.
    void rewriteTestFile() {
        std::fstream file;
        std::string newFile;
        std::string currLine;
        std::string testCaseName;
        file.open(testPath);

        for (auto& statement : testStatements) {
            if (statement->query.empty() || statement->isPartOfStatementBlock) {
                continue;
            }
            // Find `statement` in the file.
            while (getline(file, currLine)) {
                if (!currLine.starts_with("-STATEMENT")) {
                    newFile += currLine + '\n';
                    if (currLine.starts_with("-CASE")) {
                        static constexpr size_t caseNameOffset = std::string_view("-CASE ").size();
                        testCaseName = currLine.substr(caseNameOffset);
                    }
                    continue;
                }

                newFile += currLine + '\n';

                if (testCaseName != statement->testCaseName) {
                    // Not the CASE for the current `statement`.
                    continue;
                }

                std::string stmt = currLine;
                while (getline(file, currLine)) {
                    if (currLine.starts_with("----")) {
                        // "----" indicates the end of a statement.
                        break;
                    }
                    newFile += currLine + '\n';
                    // There may be other test options before the output is specified.
                    if (!currLine.starts_with("#") && !currLine.starts_with("-")) {
                        stmt += currLine;
                    }
                }
                // Manually add `connName` as `statement->query` does not retain it, if any.
                std::string connName;
                if (statement->connName.has_value() &&
                    statement->connName.value() != "conn_default") {
                    connName = "[" + statement->connName.value() + "] ";
                }

                if (removeAllSpaces(stmt) ==
                    removeAllSpaces("-STATEMENT " + connName + statement->originalQuery)) {
                    // Found the line containing `statement`.
                    break;
                }

                newFile += currLine + '\n';
            }

            // For all cases, ignore the specified expected output in the test file and append the
            // actual output instead.
            switch (statement->testResultType) {
            // `OK` results don't need anything after the dashes.
            // -STATEMENT CREATE NODE TABLE Person (ID INT64, PRIMARY KEY (ID));
            // ---- ok
            case ResultType::OK: {
                newFile += statement->newOutput;
                skipExistingOutput(file);
            } break;
            // -STATEMENT MATCH (a:person) RETURN a.fName LIMIT 4
            // ---- hash
            // 4 c921eb680e6d000e4b65556ae02361d2
            case ResultType::HASH: {
                newFile += statement->newOutput;
                // Skip existing output.
                getline(file, currLine);
            } break;
            // -CHECK_COLUMN_NAMES
            // -STATEMENT MATCH (a:person) RETURN a.fName LIMIT 2
            // ---- 3
            // a.fName
            // Alice
            // Bob
            case ResultType::TUPLES: {
                // We extract the number of expected tuples and skip over as many lines.
                constexpr size_t numTuplesPrefix = std::string_view("---- ").size();
                int linesToSkip = std::stoi(currLine.substr(numTuplesPrefix));
                std::string skippedLines;
                bool hasVariable = false;
                for (int i = 0; i < linesToSkip; ++i) {
                    getline(file, currLine);
                    skippedLines += currLine + '\n';
                    if (currLine.find("${") != std::string::npos) {
                        hasVariable = true;
                    }
                }
                // If any of the existing output tuples contain a variable, retain the output,
                // as `statement->newOutput` will replace such variables with their actual value.
                if (hasVariable) {
                    newFile += stringFormat("---- {}\n", linesToSkip);
                    newFile += skippedLines;
                } else {
                    newFile += statement->newOutput;
                }
            } break;
            // -STATEMENT MATCH (p0:person)-[r:knows]->(p1:person) RETURN ID(r)
            // ---- 5001
            // <FILE>:file_with_answers.txt
            //
            // Not supported yet.
            case ResultType::CSV_FILE: {
                newFile += currLine + '\n';
            } break;
            // Expects an error message.
            // -STATEMENT MATCH (p:person) RETURN COUNT(intended-error);
            // ---- error
            // Error: Binder exception: Variable intended is not in scope.
            case ResultType::ERROR_MSG: {
                auto lines = skipExistingOutput(file);
                // If the existing error message contains a variable, reuse the message,
                // as `statement->newOutput` will replace such variables with their actual value.
                if (lines.find("${") != std::string::npos) {
                    newFile += currLine + '\n' + lines;
                } else {
                    newFile += statement->newOutput;
                }
            } break;
            //  Expects a regex-matching error message.
            // -STATEMENT MATCH (p:person) RETURN COUNT(intended-error);
            // ---- error(regex)
            // ^Error: Binder exception: Variable .* is not in scope\.$
            case ResultType::ERROR_REGEX: {
                newFile += statement->newOutput;
                // Get the nextline which specifies the regex pattern the error should match
                getline(file, currLine);
                // If the actual output is an error, put the existing regex expression back.
                if (statement->newOutput != "---- ok\n") {
                    newFile += currLine + '\n';
                }
            } break;
            }
        }
        // Append any remaining lines in the file.
        while (getline(file, currLine)) {
            newFile += currLine + '\n';
        }
        file.close();
        file.open(testPath, std::ios::trunc | std::ios::out);
        file << newFile;
    }

    // This function removes all spaces from `s`. Used to normalize the search for a matching
    // STATEMENT when rewriting output results.
    std::string removeAllSpaces(const std::string& s) {
        std::string result;
        for (char c : s) {
            if (!std::isspace(c)) {
                result += c;
            }
        }
        return result;
    }

    // Skip all lines in `file` until an empty line, start of statement/case, or comment is found.
    std::string skipExistingOutput(std::fstream& file) {
        std::streampos lastPos;
        std::string currLine;
        std::string skippedLines;
        while (lastPos = file.tellg(), getline(file, currLine)) {
            if (currLine.empty() ||
                (currLine.starts_with("-") && !currLine.starts_with("--") &&
                    !(currLine.size() > 1 && std::isdigit(currLine[1]))) ||
                currLine.starts_with("#")) {
                file.seekg(lastPos);
                break;
            }
            skippedLines += currLine + '\n';
        }
        return skippedLines;
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
                    testStatements = std::move(testStatements), testCaseName]() mutable -> DBTest* {
                    decltype(testStatements) testStatementsCopy;
                    for (const auto& testStatement : testStatements) {
                        testStatementsCopy.emplace_back(
                            std::make_unique<TestStatement>(*testStatement));
                        testStatementsCopy.back()->testCaseName = testCaseName;
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
            spdlog::info("Starting runner in Rewrite Mode");
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
