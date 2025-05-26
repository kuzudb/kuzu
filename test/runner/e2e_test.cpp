#include <fstream>
#include <string>
#include <string_view>
#include <utility>

#include "common/assert.h"
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

    // This function collaples multiple repeating space characters into a
    // single space character.
    // This was required since in parsing for reWriteTests there were inexplicable
    // differences in whitespace causing errors.

    std::string normalize(const std::string& s) {
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
    }


    // This routine is called when E2E_REWRITE_TESTS=1 (or equivalent).
    // Note that this is a very inefficient implementation of the rewrite
    // functionality we want the runner to have.
        // 1) The testStatements vector container does not contain ALL
        // tests specified in the file.
        // Rather, it is all tests under a specified case.
        // We must find this case before we start replacing outputs.
        // See parseAndRegisterFileGroup().
        // 2) The TearDown function which invokes this routine is called every time
        // case has completed running. That is, for every case in a test file the same
        // test file is rewritten to. This is inefficient and NOT thread safe (we
        // must pass the flag TEST_JOBS=1).

    void reWriteTests() {
        std::fstream file;
        std::string newFile;
        std::string currLine;
        std::string testCaseName;
        file.open(testPath);

        for (auto& statement : testStatements) {
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

                if (testCaseName != statement->testCase) {
                    continue;
                }

                std::string stmt = currLine;

                // "----" is the prefix to a result specifer, it also indicates the end of a statement.
                while (getline(file, currLine)) {
                    if (currLine.starts_with("----")) {
                        break;
                    }
                    newFile += currLine + '\n';
                    stmt += currLine;
                }


                // For the case of multiple connections statement->query does not indicate the connName.
                // We must add it if it is required.
                std::string connName;
                if (statement->connName.has_value() && statement->connName.value() != "conn_default") {
                    connName = "[" + statement->connName.value() + "] ";
                }


                if (normalize(stmt) != (normalize("-STATEMENT " + connName + statement->query))) {
                    newFile += currLine + '\n';
                    continue;
                }

                else {
                    // All cases are handled in a similar manner; we ignore the expected
                    // output and add the produced output instead.
                    switch (statement->testResultType) {
                    // Success results don't need anything after the dashes.
                    // -STATEMENT CREATE NODE TABLE  Person (ID INT64, PRIMARY KEY (ID));
                    // ---- ok
                    case ResultType::OK: {
                        newFile += statement->newOutput;
                    } break;
                    // -STATEMENT MATCH (a:person) RETURN a.fName LIMIT 4
                    // -CHECK_ORDER # order matters with hashes
                    // ---- hash
                    // 4 c921eb680e6d000e4b65556ae02361d2
                    case ResultType::HASH: {
                        // Add result specifier.
                        newFile += currLine + '\n';
                        // Add produced hash.
                        newFile += statement->newOutput;
                        // Ignore expected hash.
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
                            // were specified.
                            static constexpr size_t numTuplesPrefix = std::string_view("---- ").size();
                            int tuplesToSkip = std::stoi(currLine.substr(numTuplesPrefix));
                            for (int i = 0; i < tuplesToSkip; ++i) {
                                getline(file, currLine);
                            }
                            // Add the produced output, which contains the
                            // updated count of tuples.
                            newFile += statement->newOutput;
                        } catch (...) {
                            // Could not overwrite the expected result.
                            // There was an error in parsing expected tuples.
                            // We are keeping the expected result as is.
                            newFile += currLine + '\n';
                        }
                    } break;
                    // -STATEMENT MATCH (p0:person)-[r:knows]->(p1:person) RETURN ID(r)
                    // ---- 5001
                    // <FILE>:file_with_answers.txt
                    case ResultType::CSV_FILE:
                        // Not supported yet.
                        { newFile += currLine + '\n'; }
                        break;
                    // Expects error message
                    // -STATEMENT MATCH (p:person) RETURN COUNT(intended-error);
                    // ---- error
                    // Error: Binder exception: Variable intended is not in scope.
                    case ResultType::ERROR_MSG: {
                        // Add the actual ouput (result and error message).
                        newFile += statement->newOutput;

                        // Ignore the expected error message. 
                        // That is, continue reading lines until the empty line
                        // or one with a '-' prefix is found.
                        std::streampos lastPos;
                        while(lastPos = file.tellg(), getline(file, currLine)) {
                            if (currLine.empty() || currLine.starts_with("-"))
                            {
                                file.seekg(lastPos);
                                break;
                            }
                        }
                    } break;
                    //  Expects regex-matching error message
                    // -STATEMENT MATCH (p:person) RETURN COUNT(intended-error);
                    // ---- error(regex)
                    // ^Error: Binder exception: Variable .* is not in scope\.$
                    case ResultType::ERROR_REGEX: {
                        // Add the produced output (i.e the result specifier line).
                        newFile += statement->newOutput;
                        // Get the nextline which specifies the regex pattern the error should match
                        getline(file, currLine);
                        // If the query still results in an error, put the existing regex expression back.
                        if (statement->newOutput != "---- ok\n")
                            newFile += currLine + '\n';
                    } break;
                    }
                    break;
                }
            }
        }
        // We get any remaining lines in the file such as comments or other 
        // statements not in the current case.
        while (getline(file, currLine)) {
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

        if (TestHelper::REWRITE_TESTS) {
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
                    testStatements = std::move(testStatements), testCaseName]() mutable -> DBTest* {
                    decltype(testStatements) testStatementsCopy;
                    for (const auto& testStatement : testStatements) {
                        testStatementsCopy.emplace_back(std::make_unique<TestStatement>(*testStatement));
                        testStatementsCopy.back()->testCase = testCaseName;
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
