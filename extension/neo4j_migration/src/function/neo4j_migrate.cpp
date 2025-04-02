#include "function/neo4j_migrate.h"

#include "binder/expression/literal_expression.h"
#include "common/exception/runtime.h"
#include "common/types/value/nested.h"
#include "function/table/bind_data.h"
#include "function/table/bind_input.h"
#include "function/table/simple_table_function.h"
#include "function/table/table_function.h"
#include "httplib.h"
#include "json.hpp"
#include "main/neo4j_migration_extension.h"

namespace kuzu {
namespace neo4j_migration_extension {

using namespace kuzu::common;
using namespace kuzu::main;
using namespace kuzu::function;

struct Neo4jMigrateBindData final : TableFuncBindData {
    std::shared_ptr<httplib::Client> client;
    std::vector<std::string> nodesToImport;
    std::vector<std::string> relsToImport;

    Neo4jMigrateBindData(std::shared_ptr<httplib::Client> client,
        std::vector<std::string> nodesToImport, std::vector<std::string> relsToImport)
        : TableFuncBindData{binder::expression_vector{}, 0 /* maxOffset */},
          client{std::move(client)}, nodesToImport{std::move(nodesToImport)},
          relsToImport{std::move(relsToImport)} {}

    std::unique_ptr<TableFuncBindData> copy() const override {
        return std::make_unique<Neo4jMigrateBindData>(*this);
    }
};

nlohmann::json executeNeo4jQuery(httplib::Client& cli, std::string neo4jQuery) {
    std::string requestBody = R"({"statements":[{"statement":"{}"}]})";
    requestBody = common::stringFormat(requestBody, neo4jQuery);
    httplib::Request req;
    req.method = "POST";
    req.path = "/db/neo4j/tx/commit";
    req.headers = {{"Accept", "application/json"}, {"Content-Type", "application/json"}};
    req.body.assign(reinterpret_cast<const char*>(requestBody.c_str()), requestBody.length());
    auto res = cli.send(req);
    auto jsonifyResult = nlohmann::json::parse(res->body);
    if (!jsonifyResult["errors"].empty()) {
        throw common::RuntimeException{
            common::stringFormat("Failed to execute query: {} in neo4j. Error: {}.", neo4jQuery,
                jsonifyResult["errors"].dump())};
    }
    if (!res || res->status != 200) {
        throw common::RuntimeException{common::stringFormat(
            "Failed to connect to neo4j. Status: {}, Response: {}.", res->status, res->body)};
    }
    return nlohmann::json::parse(res->body);
}

static void validateConnectionString(httplib::Client& cli) {
    executeNeo4jQuery(cli, "RETURN 1");
}

static std::vector<std::string> bindNodeOrRels(
    const std::shared_ptr<binder::Expression> expression) {
    std::vector<std::string> labels;
    auto labelVals = expression->constPtrCast<binder::LiteralExpression>()->getValue();
    for (auto i = 0u; i < labelVals.getChildrenSize(); i++) {
        labels.push_back(NestedVal::getChildVal(&labelVals, i)->toString());
    }
    return labels;
}

static std::unique_ptr<TableFuncBindData> bindFunc(ClientContext* context,
    const TableFuncBindInput* input) {
    auto url = input->getLiteralVal<std::string>(0);
    auto userName = input->getLiteralVal<std::string>(1);
    auto password = input->getLiteralVal<std::string>(2);
    auto nodes = bindNodeOrRels(input->getParam(3));
    auto rels = bindNodeOrRels(input->getParam(4));
    auto cli = std::make_shared<httplib::Client>(url, 7474);
    cli->set_basic_auth(userName, password);
    validateConnectionString(*cli);
    return std::make_unique<Neo4jMigrateBindData>(std::move(cli), std::move(nodes),
        std::move(rels));
}

void exportNeo4jNodeToJson(std::string nodeName, httplib::Client& cli) {
    auto query =
        common::stringFormat("MATCH (p:{}) "
                             "with collect(p) as n "
                             "CALL apoc.export.json.data(n, [], \\\"/tmp/{}.json\\\", null) "
                             "YIELD file, source, format, nodes, relationships, "
                             "properties, time, rows, batchSize, batches, done, data "
                             "RETURN file, source, format, nodes, relationships, "
                             "properties, time, rows, batchSize, batches, done, data",
            nodeName, nodeName);
    executeNeo4jQuery(cli, query);
}

void exportNeo4jRelToJson(std::string relName, std::pair<std::string, std::string> nodePairs,
    httplib::Client& cli) {
    auto query = common::stringFormat(
        "MATCH (:{})-[e:{}]->(:{}) "
        "with collect(e) as rels "
        "CALL apoc.export.json.data([], rels, \\\"/tmp/{}_{}_{}.json\\\", null) "
        "YIELD file, source, format, nodes, relationships, "
        "properties, time, rows, batchSize, batches, done, data "
        "RETURN file, source, format, nodes, relationships, "
        "properties, time, rows, batchSize, batches, done, data",
        nodePairs.first, relName, nodePairs.second, nodePairs.first, relName, nodePairs.second);
    executeNeo4jQuery(cli, query);
}

LogicalType convertFromNeo4jTypeStr(const std::string& neo4jTypeStr) {
    if (neo4jTypeStr == "Long") {
        return LogicalType{LogicalTypeID::INT64};
    } else if (neo4jTypeStr == "String") {
        return LogicalType{LogicalTypeID::STRING};
    } else {
        KU_UNREACHABLE;
    }
}

std::pair<std::string, std::string> getCreateNodeTableQuery(httplib::Client& cli,
    const std::string& nodeName) {
    auto neo4jQuery = common::stringFormat(
        "call db.schema.nodeTypeProperties() yield nodeType, propertyName,propertyTypes where "
        "nodeType = ':`{}`' return propertyName,propertyTypes",
        nodeName);
    auto data = executeNeo4jQuery(cli, neo4jQuery);
    std::string properties = "";
    std::string propertiesToCopy = "";
    for (const auto& result : data["results"]) {
        for (const auto& item : result["data"]) {
            if (!item["row"].empty()) {
                auto property = item["row"][0].get<std::string>();
                properties += property;
                auto types = item["row"][1];
                auto kuType = convertFromNeo4jTypeStr(types[0].get<std::string>());
                for (auto i = 1u; i < types.size(); i++) {
                    kuType = LogicalTypeUtils::combineTypes(
                        convertFromNeo4jTypeStr(types[i].get<std::string>()), kuType);
                }
                propertiesToCopy += common::stringFormat(
                    "cast(struct_extract(`properties`, '{}') as {}),", property, kuType.toString());
                properties += (" " + kuType.toString() + ",");
            }
        }
    }
    return {common::stringFormat("CREATE NODE TABLE {} (_id_ int64, {} PRIMARY KEY(_id_));",
                nodeName, properties),
        common::stringFormat(
            "COPY {} FROM (LOAD FROM '/tmp/{}.json' RETURN cast(id as int64), {});", nodeName,
            nodeName, propertiesToCopy.substr(0, propertiesToCopy.length() - 1))};
}

std::vector<std::string> getRelProperties(httplib::Client& cli, std::string srcLabel,
    std::string dstLabel, std::string relLabel) {
    auto neo4jQuery =
        common::stringFormat("MATCH (:{})-[e:{}]->(:{}) UNWIND keys(e) AS key return distinct key",
            srcLabel, relLabel, dstLabel);
    auto data = executeNeo4jQuery(cli, neo4jQuery);
    std::vector<std::string> relProperties;
    for (auto& row : data["results"][0]["data"][0]["row"]) {
        relProperties.push_back(row.get<std::string>());
    }
    return relProperties;
}

std::string getCreateRelTableQuery(httplib::Client& cli, const std::string& relName) {
    auto neo4jQuery = common::stringFormat(
        "call db.schema.relTypeProperties() yield relType, propertyName,propertyTypes where "
        "relType = ':`{}`' return propertyName,propertyTypes",
        relName);
    auto data = executeNeo4jQuery(cli, neo4jQuery);
    std::string properties = "";
    std::unordered_map<std::string, LogicalType> propertyTypes;
    for (const auto& result : data["results"]) {
        for (const auto& item : result["data"]) {
            if (!item["row"].empty()) {
                auto property = item["row"][0].get<std::string>();
                properties += property;
                auto types = item["row"][1];
                auto kuType = convertFromNeo4jTypeStr(types[0].get<std::string>());
                for (auto i = 1u; i < types.size(); i++) {
                    kuType = LogicalTypeUtils::combineTypes(
                        convertFromNeo4jTypeStr(types[i].get<std::string>()), kuType);
                }
                properties += (" " + kuType.toString() + ",");
                propertyTypes.emplace(property)
            }
        }
    }

    neo4jQuery =
        common::stringFormat("MATCH (a)-[:{}]->(b) RETURN distinct labels(a), labels(b);", relName);
    data = executeNeo4jQuery(cli, neo4jQuery);
    std::vector<std::pair<std::string, std::string>> nodePairs;
    std::string srcLabel, dstLabel;
    std::string nodePairsString;
    std::string copyQuery;
    for (const auto& result : data["results"]) {
        for (const auto& item : result["data"]) {
            if (item["row"].empty()) {
                throw common::RuntimeException{"Error occurred while parsing neo4j result."};
            }
            srcLabel = item["row"][0][0].get<std::string>();
            dstLabel = item["row"][1][0].get<std::string>();
            nodePairs.emplace_back(srcLabel, dstLabel);
            nodePairsString += common::stringFormat("FROM {} TO {},", srcLabel, dstLabel);
            exportNeo4jRelToJson(relName, {srcLabel, dstLabel}, cli);
            auto relProperties = getRelProperties(cli, srcLabel, dstLabel, relName);

            std::string propertiesToCopy = "";
            std::string propertiesToExtract = "";
            for (auto i = 0u; i < relProperties.size(); i++) {
                propertiesToCopy += relProperties[i];
                propertiesToExtract += common::stringFormat("cast(struct_extract() as {})", );
                if (i != relProperties.size() - 1) {
                    propertiesToCopy += ",";
                    propertiesToExtract += ",";
                }
            }
            copyQuery += common::stringFormat(
                "COPY {}({}) FROM (LOAD FROM '/tmp/{}_{}_{}.json' RETURN "
                "cast(struct_extract(`start`, 'id') as int64), "
                "cast(struct_extract(`end`, 'id') as int64), {}) (from = \"{}\", to = \"{}\");",
                relName, propertiesToCopy, srcLabel, relName, dstLabel, propertiesToExtract,
                srcLabel, dstLabel);
        }
    }
    return common::stringFormat("CREATE REL TABLE {} ({} {});", relName, nodePairsString,
               properties.substr(0, properties.length() - 1)) +
           copyQuery;
}

std::string migrateQuery(ClientContext& context, const TableFuncBindData& bindData) {
    std::string result;
    auto neo4jMigrateBindData = bindData.constPtrCast<Neo4jMigrateBindData>();
    for (auto node : neo4jMigrateBindData->nodesToImport) {
        exportNeo4jNodeToJson(node, *neo4jMigrateBindData->client);
        auto [ddl, copyQuery] = getCreateNodeTableQuery(*neo4jMigrateBindData->client, node);
        result += ddl;
        result += copyQuery;
    }
    for (auto rel : neo4jMigrateBindData->relsToImport) {
        result += getCreateRelTableQuery(*neo4jMigrateBindData->client, rel);
    }
    return result;
}

static std::vector<common::LogicalType> inferInputTypes(
    const binder::expression_vector& /*params*/) {
    std::vector<common::LogicalType> inputTypes;
    // url
    inputTypes.push_back(common::LogicalType::STRING());
    // username
    inputTypes.push_back(common::LogicalType::STRING());
    // password
    inputTypes.push_back(common::LogicalType::STRING());
    // nodes
    inputTypes.push_back(common::LogicalType::LIST(common::LogicalType::STRING()));
    // rels
    inputTypes.push_back(common::LogicalType::LIST(common::LogicalType::STRING()));
    return inputTypes;
}

function_set Neo4jMigrateFunction::getFunctionSet() {
    function_set functionSet;
    auto func = std::make_unique<TableFunction>(name,
        std::vector{LogicalTypeID::STRING, LogicalTypeID::STRING, LogicalTypeID::STRING,
            LogicalTypeID::LIST, LogicalTypeID::LIST});
    func->tableFunc = TableFunction::emptyTableFunc;
    func->bindFunc = bindFunc;
    func->initSharedStateFunc = SimpleTableFunc::initSharedState;
    func->initLocalStateFunc = TableFunction::initEmptyLocalState;
    func->rewriteFunc = migrateQuery;
    func->canParallelFunc = [] { return false; };
    func->inferInputTypes = inferInputTypes;
    functionSet.push_back(std::move(func));
    return functionSet;
}

} // namespace neo4j_migration_extension
} // namespace kuzu
