#include "test_runner/csv_to_parquet_converter.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/file_system/local_file_system.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

void CSVToParquetConverter::copySchemaFile() {
    LocalFileSystem localFileSystem;
    auto csvSchemaFile =
        localFileSystem.joinPath(csvDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME));
    auto parquetSchemaFile =
        localFileSystem.joinPath(parquetDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME));
    if (!localFileSystem.fileOrPathExists(parquetSchemaFile)) {
        localFileSystem.copyFile(csvSchemaFile, parquetSchemaFile);
    } else {
        localFileSystem.overwriteFile(csvSchemaFile, parquetSchemaFile);
    }
    createTableInfo(parquetSchemaFile);
}

void CSVToParquetConverter::createTableInfo(std::string schemaFile) {
    std::ifstream file(schemaFile);
    if (!file.is_open()) {
        throw TestException(stringFormat("Error opening file: {}, errno: {}.", schemaFile, errno));
    }
    // This implementation stays as a temporary solution to create copy statements for rel tables
    // We'll switch to use table_info once that function can provide everything needed
    // table_info is mentioned in this issue https://github.com/kuzudb/kuzu/issues/2991
    std::string line;
    while (getline(file, line)) {
        auto tokens = StringUtils::split(line, " ");

        std::transform(tokens[0].begin(), tokens[0].end(), tokens[0].begin(),
            [](unsigned char c) { return std::tolower(c); });
        std::transform(tokens[2].begin(), tokens[2].end(), tokens[2].begin(),
            [](unsigned char c) { return std::tolower(c); });
        if (tokens[0] != "create" || tokens[2] != "table") {
            throw TestException(stringFormat("Invalid CREATE statement: {}", line));
        }

        auto tableType = tokens[1];
        std::transform(tableType.begin(), tableType.end(), tableType.begin(),
            [](unsigned char c) { return std::tolower(c); });
        auto tableName = tokens[3];

        std::shared_ptr<TableInfo> table;
        if (tableType == "node") {
            auto nodeTable = std::make_shared<NodeTableInfo>();
            size_t primaryKeyPos = line.find("PRIMARY KEY");
            if (primaryKeyPos != std::string::npos) {
                size_t openParenPos = line.find("(", primaryKeyPos);
                size_t closeParenPos = line.find(")", primaryKeyPos);
                if (openParenPos != std::string::npos && closeParenPos != std::string::npos &&
                    openParenPos < closeParenPos) {
                    nodeTable->primaryKey =
                        line.substr(openParenPos + 1, closeParenPos - openParenPos - 1);
                    table = nodeTable;
                } else {
                    throw TestException(
                        stringFormat("PRIMARY KEY is not defined in node table: {}", line));
                }
            } else {
                throw TestException(
                    stringFormat("PRIMARY KEY is not defined in node table: {}", line));
            }
        } else {
            auto relTable = std::make_shared<RelTableInfo>();
            size_t startPos = line.find("FROM");
            if (startPos != std::string::npos) {
                size_t endPos = line.find_first_of(",)", startPos);
                if (endPos != std::string::npos) {
                    auto tmp = StringUtils::splitBySpace(line.substr(startPos, endPos - startPos));
                    relTable->fromTable =
                        std::dynamic_pointer_cast<NodeTableInfo>(tableNameMap[tmp[1]]);
                    relTable->toTable =
                        std::dynamic_pointer_cast<NodeTableInfo>(tableNameMap[tmp[3]]);
                    table = relTable;
                } else {
                    throw TestException(stringFormat(
                        "FROM node and TO node are not defined in rel table: {}", line));
                }
            } else {
                throw TestException(
                    stringFormat("FROM node and TO node are not defined in rel table: {}", line));
            }
        }
        table->name = tableName;
        tables.push_back(table);
        tableNameMap[tableName] = table;
    }
}

std::string extractPath(std::string& str, char delimiter) {
    std::string::size_type posStart = str.find_first_of(delimiter);
    std::string::size_type posEnd = str.find_last_of(delimiter);
    return str.substr(posStart + 1, posEnd - posStart - 1);
}

void CSVToParquetConverter::readCopyCommandsFromCSVDataset() {
    auto csvCopyFile =
        LocalFileSystem::joinPath(csvDatasetPath, std::string(TestHelper::COPY_FILE_NAME));
    std::ifstream file(csvCopyFile);
    if (!file.is_open()) {
        throw TestException(stringFormat("Error opening file: {}, errno: {}.", csvCopyFile, errno));
    }
    std::string line;
    while (getline(file, line)) {
        auto tokens = StringUtils::split(line, " ");
        auto path = std::filesystem::path(extractPath(tokens[3], '"'));
        auto table = tableNameMap[tokens[1]];
        table->csvFilePath = TestHelper::appendKuzuRootPath(path.string());
        auto parquetFileName = path.stem().string() + ".parquet";
        table->parquetFilePath = parquetDatasetPath + "/" + parquetFileName;
    }
}

void CSVToParquetConverter::createCopyFile() {
    readCopyCommandsFromCSVDataset();
    auto parquetCopyFile =
        LocalFileSystem::joinPath(parquetDatasetPath, std::string(TestHelper::COPY_FILE_NAME));
    std::ofstream outfile(parquetCopyFile);
    if (!outfile.is_open()) {
        throw TestException(
            stringFormat("Error opening file: {}, errno: {}.", parquetCopyFile, errno));
    }
    std::string kuzuRootPath = KUZU_ROOT_DIRECTORY + std::string("/");
    for (auto table : tables) {
        auto cmd = stringFormat("COPY {} FROM \"{}\";", table->name,
            table->parquetFilePath.substr(kuzuRootPath.length()));
        outfile << cmd << '\n';
    }
}

void CSVToParquetConverter::convertCSVFilesToParquet() {
    // Load CSV Files to temp database
    TestHelper::executeScript(
        LocalFileSystem::joinPath(csvDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME)),
        *tempConn);
    TestHelper::executeScript(
        LocalFileSystem::joinPath(csvDatasetPath, std::string(TestHelper::COPY_FILE_NAME)),
        *tempConn);

    spdlog::set_level(spdlog::level::info);
    for (auto table : tables) {
        spdlog::info("Converting: {} to {}", table->csvFilePath, table->parquetFilePath);
        auto cmd = table->getConverterQuery();
        tempConn->query(cmd);
        spdlog::info("Executed query: {}", cmd);
    }
}

void CSVToParquetConverter::convertCSVDatasetToParquet() {
    LocalFileSystem localFileSystem;
    if (!localFileSystem.fileOrPathExists(parquetDatasetPath)) {
        localFileSystem.createDir(parquetDatasetPath);
    }

    copySchemaFile();
    createCopyFile();

    systemConfig = std::make_unique<main::SystemConfig>(bufferPoolSize);
    std::string tempDatabasePath = TestHelper::appendKuzuRootPath(
        std::string(TestHelper::TMP_TEST_DIR) + "csv_to_parquet_converter_" +
        TestHelper::getMillisecondsSuffix());
    tempDb = std::make_unique<main::Database>(tempDatabasePath, *systemConfig);
    tempConn = std::make_unique<main::Connection>(tempDb.get());

    convertCSVFilesToParquet();
    std::filesystem::remove_all(tempDatabasePath);
}

std::string CSVToParquetConverter::NodeTableInfo::getConverterQuery() const {
    return stringFormat("COPY (MATCH (a:{}) RETURN a.*) TO \"{}\";", name, parquetFilePath);
}

std::string CSVToParquetConverter::RelTableInfo::getConverterQuery() const {
    return stringFormat("COPY (MATCH (a)-[e:{}]->(b) RETURN a.{}, b.{}, e.*) TO \"{}\";", name,
        fromTable->primaryKey, toTable->primaryKey, parquetFilePath);
}

} // namespace testing
} // namespace kuzu
