#include "test_runner/insert_by_row.h"

#include <fstream>

#include "common/exception/test.h"
#include "common/string_utils.h"
#include "test_helper/test_helper.h"
#include "test_runner/csv_converter.h"

using namespace kuzu::common;

namespace kuzu {
namespace testing {

static std::unique_ptr<main::QueryResult> validateQuery(main::Connection& conn,
    std::string& query) {
    auto result = conn.query(query);
    if (!result->isSuccess()) {
        throw Exception(stringFormat("Failed to execute statement: {}.\nError: {}", query,
            result->getErrorMessage()));
    }
    return result;
}

void InsertDatasetByRow::init() {
    auto copyFile = "dataset/" + datasetPath + "/" + TestHelper::COPY_FILE_NAME;
    copyFile = TestHelper::appendKuzuRootPath(copyFile);
    std::ifstream file(copyFile);
    if (!file.is_open()) {
        throw TestException(stringFormat("Error opening file: {}, errno: {}.", copyFile, errno));
    }
    std::string line;
    while (getline(file, line)) {
        auto tokens = StringUtils::split(line, " ");
        auto copyStatement = tokens[0];
        auto tableName = tokens[1];
        StringUtils::toLower(copyStatement);
        if (copyStatement != "copy") {
            throw TestException(stringFormat("Invalid COPY statement: {}", line));
        }
        auto query = stringFormat("call show_tables() where name='{}' return type;", tableName);
        auto result = validateQuery(connection, query);
        auto tableType = result->getNext()->getValue(0)->toString();
        query = stringFormat("call table_info('{}') return name, type order by `property id`;",
            tableName);
        result = validateQuery(connection, query);
        std::vector<std::pair<std::string, std::string>> properties;
        while (result->hasNext()) {
            auto tuple = result->getNext();
            properties.emplace_back(tuple->getValue(0)->toString(), tuple->getValue(1)->toString());
        }
        size_t start = line.find('"', 0);
        size_t end = line.find('"', start + 1);
        if (start == std::string::npos || end == std::string::npos) {
            throw TestException(stringFormat("Invalid file from copy {}.", line));
        }
        std::string filePath = line.substr(start + 1, end - start - 1);
        auto fullPath = TestHelper::appendKuzuRootPath(filePath);
        line.replace(line.find(filePath), filePath.length(), fullPath);
        end = line.find(';');
        auto datasetFilePath = line.substr(start, end == std::string::npos ? end : end - start);
        if (tableType == "NODE") {
            tables.push_back(std::make_unique<NodeTableInfo>(std::move(tableName),
                std::move(datasetFilePath), std::move(properties)));
        } else if (tableType == "REL") {
            query = stringFormat("call show_connection('{}') return *;", tableName);
            result = validateQuery(connection, query);
            auto tuple = result->getNext();
            RelConnection from;
            RelConnection to;
            from.name = tuple->getValue(0)->toString();
            from.property = tuple->getValue(2)->toString();
            to.name = tuple->getValue(1)->toString();
            to.property = tuple->getValue(3)->toString();
            query = stringFormat("call table_info('{}') where name='{}' return type;"
                                 "call table_info('{}') where name='{}' return type;",
                from.name, from.property, to.name, to.property);
            result = validateQuery(connection, query);
            from.type = result->getNext()->getValue(0)->toString();
            auto next = result->getNextQueryResult();
            to.type = next->getNext()->getValue(0)->toString();
            tables.push_back(std::make_unique<RelTableInfo>(std::move(tableName),
                std::move(datasetFilePath), std::move(properties), std::move(from), std::move(to)));
        } else {
            throw Exception(stringFormat("Unsupported table type {} for insert by row", tableName,
                result->getErrorMessage()));
        }
    }
}

void InsertDatasetByRow::run() {
    for (auto& table : tables) {
        auto query = table->getLoadFromQuery();
        validateQuery(connection, query);
    }
}

std::string InsertDatasetByRow::TableInfo::getHeaderForLoad() const {
    std::vector<std::string> strVec;
    for (auto& prop : properties) {
        strVec.push_back(prop.first + " " + prop.second);
    }
    return StringUtils::join(strVec, ",");
}

std::string InsertDatasetByRow::TableInfo::getBodyForLoad() const {
    std::vector<std::string> strVec;
    for (auto& prop : properties) {
        strVec.push_back(prop.first + ":" + prop.first);
    }
    return StringUtils::join(strVec, ",");
}

std::string InsertDatasetByRow::NodeTableInfo::getLoadFromQuery() const {
    const std::string query = "LOAD WITH HEADERS ({}) FROM {} "
                              "CREATE (:{} {});";
    const auto header = getHeaderForLoad();
    const auto body = getBodyForLoad();
    return stringFormat(query, header, filePath, name, "{" + body + "}");
}

std::string InsertDatasetByRow::RelTableInfo::getLoadFromQuery() const {
    const std::string query = "LOAD WITH HEADERS ({}) FROM {} "
                              "MATCH (a:{}), (b:{}) WHERE a.{} = aid_ AND b.{} = bid_ "
                              "CREATE (a)-[:{} {}]->(b);";
    auto header = stringFormat("aid_ {},bid_ {}", from.type, to.type);
    auto headerRest = getHeaderForLoad();
    header += headerRest.length() == 0 ? "" : "," + getHeaderForLoad();
    const auto body = getBodyForLoad();
    return stringFormat(query, header, filePath, from.name, to.name, from.property, to.property,
        name, "{" + body + "}");
}

} // namespace testing
} // namespace kuzu
