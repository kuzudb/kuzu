#include "test_runner/csv_converter.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/file_system/local_file_system.h"
#include "common/string_utils.h"
#include "spdlog/spdlog.h"
#include "test_helper/test_helper.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

void CSVConverter::copySchemaFile() {
    LocalFileSystem localFileSystem("");
    auto csvSchemaFile =
        localFileSystem.joinPath(csvDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME));
    auto outputSchemaFile =
        localFileSystem.joinPath(outputDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME));
    if (!localFileSystem.fileOrPathExists(outputSchemaFile)) {
        localFileSystem.copyFile(csvSchemaFile, outputSchemaFile);
    } else {
        localFileSystem.overwriteFile(csvSchemaFile, outputSchemaFile);
    }
    createTableInfo(outputSchemaFile);
}

void CSVConverter::createTableInfo(std::string schemaFile) {
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

void CSVConverter::readCopyCommandsFromCSVDataset() {
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
        auto outputFileName = path.stem().string() + fileExtension;
        table->outputFilePath = outputDatasetPath + "/" + outputFileName;
    }
}

void CSVConverter::createCopyFile() {
    readCopyCommandsFromCSVDataset();
    auto outputCopyFile =
        LocalFileSystem::joinPath(outputDatasetPath, std::string(TestHelper::COPY_FILE_NAME));
    std::ofstream outfile(outputCopyFile);
    if (!outfile.is_open()) {
        throw TestException(
            stringFormat("Error opening file: {}, errno: {}.", outputCopyFile, errno));
    }
    if (fileExtension == ".json") {
        outfile << "load extension \"" + TestHelper::appendKuzuRootPath(
                                             "extension/json/build/libjson.kuzu_extension\"\n");
    }
    for (auto table : tables) {
        auto cmd = stringFormat("COPY {} FROM \"{}\";", table->name, table->outputFilePath);
        outfile << cmd << '\n';
    }
}

void CSVConverter::convertCSVFiles() {
    // Load CSV Files to temp database
    TestHelper::executeScript(
        LocalFileSystem::joinPath(csvDatasetPath, std::string(TestHelper::SCHEMA_FILE_NAME)),
        *tempConn);
    TestHelper::executeScript(
        LocalFileSystem::joinPath(csvDatasetPath, std::string(TestHelper::COPY_FILE_NAME)),
        *tempConn);

    spdlog::set_level(spdlog::level::info);
    if (fileExtension == ".json") {
        auto result = tempConn->query(
            "load extension \"" +
            TestHelper::appendKuzuRootPath("extension/json/build/libjson.kuzu_extension\""));
        if (!result->isSuccess()) {
            spdlog::error(result->getErrorMessage());
        }
    }
    for (auto table : tables) {
        spdlog::info("Converting: {} to {}", table->csvFilePath, table->outputFilePath);
        auto cmd = table->getConverterQuery();
        auto result = tempConn->query(cmd);
        if (!result->isSuccess()) {
            spdlog::error(result->getErrorMessage());
        } else {
            spdlog::info("Executed query: {}", cmd);
        }
    }
}

void CSVConverter::convertCSVDataset() {
    LocalFileSystem localFileSystem("");
    if (!localFileSystem.fileOrPathExists(outputDatasetPath)) {
        localFileSystem.createDir(outputDatasetPath);
    }

    copySchemaFile();
    createCopyFile();

    systemConfig = TestHelper::getSystemConfigFromEnv();
    // FIXME(bmwinger): This ignores the environment variables
    systemConfig->bufferPoolSize = bufferPoolSize;
    std::string tempDatabasePath = TestHelper::getTempDir("csv_converter");
    tempDb = std::make_unique<main::Database>(tempDatabasePath, *systemConfig);
    tempConn = std::make_unique<main::Connection>(tempDb.get());

    convertCSVFiles();
    std::filesystem::remove_all(tempDatabasePath);
}

std::string CSVConverter::NodeTableInfo::getConverterQuery() const {
    return stringFormat("COPY (MATCH (a:{}) RETURN a.*) TO \"{}\";", name, outputFilePath);
}

std::string CSVConverter::RelTableInfo::getConverterQuery() const {
    return stringFormat(
        "COPY (MATCH (a)-[e:{}]->(b) RETURN a.{} AS `from`, b.{} AS `to`, e.*) TO \"{}\";", name,
        fromTable->primaryKey, toTable->primaryKey, outputFilePath);
}

} // namespace testing
} // namespace kuzu
