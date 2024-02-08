#include "binder/binder.h"
#include "binder/copy/bound_export_database.h"
#include "binder/expression/expression.h"
#include "binder/expression/property_expression.h"
#include "catalog/catalog.h"
#include "catalog/rel_table_schema.h"
#include "catalog/table_schema.h"
#include "planner/operator/extend/logical_extend.h"
#include "planner/operator/persistent/logical_copy_to.h"
#include "planner/operator/persistent/logical_export_db.h"
#include "planner/operator/scan/logical_scan_internal_id.h"
#include "planner/planner.h"

using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::catalog;
using namespace kuzu::common;
using namespace kuzu::transaction;

namespace kuzu {
namespace planner {

static std::shared_ptr<Expression> createPropertyExpression(const std::string& propertyName,
    const std::string& uniqueVariableName, const std::string& rawVariableName,
    const std::vector<TableSchema*>& tableSchemas) {
    bool isPrimaryKey = false;
    if (tableSchemas.size() == 1 && tableSchemas[0]->tableType == TableType::NODE) {
        auto nodeTableSchema = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(tableSchemas[0]);
        isPrimaryKey = nodeTableSchema->getPrimaryKeyPropertyID() ==
                       nodeTableSchema->getPropertyID(propertyName);
    }
    std::unordered_map<common::table_id_t, common::property_id_t> tableIDToPropertyID;
    std::vector<const LogicalType*> propertyDataTypes;
    for (auto& tableSchema : tableSchemas) {
        if (!tableSchema->containProperty(propertyName)) {
            continue;
        }
        auto propertyID = tableSchema->getPropertyID(propertyName);
        propertyDataTypes.push_back(tableSchema->getProperty(propertyID)->getDataType());
        tableIDToPropertyID.insert({tableSchema->getTableID(), propertyID});
    }
    return make_shared<PropertyExpression>(*propertyDataTypes[0], propertyName, uniqueVariableName,
        rawVariableName, tableIDToPropertyID, isPrimaryKey);
}

void appendScanInternalIDToPlan(
    table_id_t tableID, std::shared_ptr<Expression> internalID, LogicalPlan& plan) {
    std::vector<table_id_t> tableIDs;
    tableIDs.push_back(tableID);
    auto scan = make_shared<LogicalScanInternalID>(std::move(internalID), std::move(tableIDs));
    scan->computeFactorizedSchema();
    // update cardinality
    plan.setCardinality(0);
    plan.setLastOperator(std::move(scan));
}

std::unique_ptr<PropertyExpression> getInternalIDExpression(
    table_id_t tableID, std::string uniqueVariableName) {
    std::vector<table_id_t> tableIDs;
    tableIDs.push_back(tableID);
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    return std::make_unique<PropertyExpression>(LogicalType(LogicalTypeID::INTERNAL_ID),
        InternalKeyword::ID, uniqueVariableName, uniqueVariableName, std::move(propertyIDPerTable),
        false /* isPrimaryKey */);
}

std::shared_ptr<NodeExpression> getNodeExpression(table_id_t tableID, std::string uniqueName) {
    std::vector<table_id_t> tableIDs;
    tableIDs.push_back(tableID);
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    return std::make_shared<NodeExpression>(
        LogicalType(LogicalTypeID::NODE), uniqueName, uniqueName, tableIDs);
}

std::shared_ptr<RelExpression> getRelExpression(table_id_t tableID,
    std::shared_ptr<NodeExpression> srcNode, std::shared_ptr<NodeExpression> dstNode,
    std::string uniqueName) {
    std::vector<table_id_t> tableIDs;
    tableIDs.push_back(tableID);
    std::unordered_map<table_id_t, property_id_t> propertyIDPerTable;
    propertyIDPerTable.insert({tableID, INVALID_PROPERTY_ID});
    return std::make_shared<RelExpression>(LogicalType(LogicalTypeID::INTERNAL_ID), uniqueName,
        uniqueName, tableIDs, srcNode, dstNode, RelDirectionType::UNKNOWN,
        QueryRelType::NON_RECURSIVE);
}

std::vector<std::string> getTableCypher(catalog::Catalog* catalog) {
    std::vector<std::string> tableCyphers;
    for (auto& schema : catalog->getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto nodeSchema = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema);
        tableCyphers.push_back(nodeSchema->ToCypher());
    }
    for (auto& schema : catalog->getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto relSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
        auto srcTableName =
            catalog->getTableName(&DUMMY_READ_TRANSACTION, relSchema->getSrcTableID());
        auto dstTableName =
            catalog->getTableName(&DUMMY_READ_TRANSACTION, relSchema->getDstTableID());
        tableCyphers.push_back(relSchema->ToCypher(srcTableName, dstTableName));
    }
    return tableCyphers;
}

std::vector<std::string> getMacroCypher(catalog::Catalog* catalog) {
    std::vector<std::string> macroCyphers;
    for (auto macroName : catalog->getMacroNames(&DUMMY_READ_TRANSACTION)) {
        macroCyphers.push_back(catalog->getScalarMacroFunction(macroName)->ToCypher(macroName));
    }
    return macroCyphers;
}

std::vector<std::string> getTableNames(catalog::Catalog* catalog) {
    std::vector<std::string> tableNames;
    for (auto& schema : catalog->getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        tableNames.push_back(schema->getName());
    }
    for (auto& schema : catalog->getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        tableNames.push_back(schema->getName());
    }
    return tableNames;
}

void getColumnInfo(std::vector<common::LogicalType>& columnTypes,
    std::vector<std::string>& columnNames, TableSchema* schema) {
    auto& properties = schema->getPropertiesRef();
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        auto dataType = property.getDataType();
        if (dataType->getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
            continue;
        }
        auto propName = property.getName();
        columnTypes.push_back(*dataType);
        auto colName = StorageUtils::getColumnName(
            property.getName(), StorageUtils::ColumnType::DEFAULT, "fwd" /*prefix*/);
        columnNames.push_back(colName);
    }
}

void getProp(std::shared_ptr<binder::expression_vector> expProperties, TableSchema* schema) {
    auto& properties = schema->getPropertiesRef();
    for (auto i = 0u; i < properties.size(); i++) {
        auto& property = properties[i];
        auto dataType = property.getDataType();
        if (dataType->getLogicalTypeID() == LogicalTypeID::INTERNAL_ID) {
            continue;
        }
        auto propName = property.getName();
        auto propExp = createPropertyExpression(property.getName(), propName /*uniqueVariableName*/,
            propName /*rawVariableName*/, {schema} /*tableSchemas*/);
        expProperties->push_back(propExp);
    }
}

void getPrimaryKeyProp(std::shared_ptr<binder::expression_vector> expProperties, table_id_t tableId,
    Catalog* catalog, std::string prefix) {
    auto schema = catalog->getTableSchema(&DUMMY_READ_TRANSACTION, tableId);
    auto primaryProperty = ku_dynamic_cast<TableSchema*, NodeTableSchema*>(schema)->getPrimaryKey();
    auto propName = primaryProperty->getName();
    auto propExp =
        createPropertyExpression(propName, propName + "_primary_key" + schema->getName() + prefix,
            propName + "_primary_key" + schema->getName() + prefix, {schema} /*tableSchemas*/);
    expProperties->push_back(propExp);
}

std::shared_ptr<LogicalOperator> Planner::appendRelForExportDB(
    TableSchema* schema, Catalog* catalog) {
    auto plan = std::make_unique<LogicalPlan>();
    auto tableName = schema->tableName;
    // get prop info
    auto relSchema = ku_dynamic_cast<TableSchema*, RelTableSchema*>(schema);
    // boundNode
    auto planProbe = std::make_unique<LogicalPlan>();
    auto boundNode =
        getNodeExpression(relSchema->getSrcTableID(), "exportDBBoundNode_" + tableName);
    auto boundNodeInternalID =
        getInternalIDExpression(schema->tableID, "exportDBNodeID_" + tableName);
    boundNode->setInternalID(std::move(boundNodeInternalID));
    appendScanInternalIDToPlan(relSchema->getSrcTableID(), boundNode->getInternalID(), *planProbe);

    auto boundNodeExpProperties = std::make_shared<binder::expression_vector>();
    getPrimaryKeyProp(boundNodeExpProperties, relSchema->getSrcTableID(), catalog, "src");
    expression_vector& nodeProp = *boundNodeExpProperties;
    appendScanNodeProperties(
        boundNode->getInternalID(), {relSchema->getSrcTableID()}, nodeProp, *planProbe);

    // dstNode
    auto planBuild = std::make_unique<LogicalPlan>();
    auto nbrNode = getNodeExpression(relSchema->getDstTableID(), "exportDBNbrNode_" + tableName);
    auto nbrNodeInternalID =
        getInternalIDExpression(relSchema->getDstTableID(), "exportDBNbrNodeID_" + tableName);
    nbrNode->setInternalID(std::move(nbrNodeInternalID));
    appendScanInternalIDToPlan(relSchema->getDstTableID(), nbrNode->getInternalID(), *planBuild);

    auto nbrNodeExpProperties = std::make_shared<binder::expression_vector>();
    getPrimaryKeyProp(nbrNodeExpProperties, relSchema->getDstTableID(), catalog, "nbr");
    expression_vector& nbrNodeProp = *nbrNodeExpProperties;
    appendScanNodeProperties(
        nbrNode->getInternalID(), {relSchema->getDstTableID()}, nbrNodeProp, *planBuild);

    // rel
    auto rel =
        getRelExpression(relSchema->getTableID(), boundNode, nbrNode, "exportDBRel_" + tableName);
    auto relExpProperties = std::make_shared<binder::expression_vector>();
    getProp(relExpProperties, schema);
    expression_vector& relProp = *relExpProperties;
    appendNonRecursiveExtend(nbrNode, boundNode, rel, ExtendDirection::BWD, relProp, *planBuild);

    // append hash join
    appendHashJoin(
        expression_vector{boundNode->getInternalID()}, JoinType::INNER, *planProbe, *planBuild);

    // project prop
    auto projectExpProperties = std::make_shared<binder::expression_vector>();
    getPrimaryKeyProp(projectExpProperties, relSchema->getSrcTableID(), catalog, "src");
    getPrimaryKeyProp(projectExpProperties, relSchema->getDstTableID(), catalog, "nbr");
    getProp(projectExpProperties, schema);
    expression_vector& projectProp = *projectExpProperties;
    appendProjection(projectProp, *planProbe);
    return planProbe->getLastOperator();
}

std::unique_ptr<LogicalPlan> Planner::planExportDatabase(const BoundStatement& statement) {
    auto& boundExportDatabase =
        ku_dynamic_cast<const BoundStatement&, const BoundExportDatabase&>(statement);
    auto filePath = boundExportDatabase.getFilePath();
    auto fileType = boundExportDatabase.getFileType();
    auto logicalOperators = std::vector<std::shared_ptr<LogicalOperator>>();
    binder::expression_vector exp_properties;
    auto plan = std::make_unique<LogicalPlan>();
    // create node copyTO operator
    for (auto& schema : catalog->getNodeTableSchemas(&DUMMY_READ_TRANSACTION)) {
        std::vector<std::string> columnNames;
        std::vector<common::LogicalType> columnTypes;
        auto expProperties = std::make_shared<binder::expression_vector>();
        getColumnInfo(columnTypes, columnNames, schema);
        getProp(expProperties, schema);
        expression_vector& refProp = *expProperties;
        std::shared_ptr<PropertyExpression> nodeInternalID =
            getInternalIDExpression(schema->tableID, "exportDBNodeID_" + schema->tableName);
        appendScanInternalIDToPlan(schema->tableID, nodeInternalID, *plan);
        appendScanNodeProperties(nodeInternalID, {schema->tableID}, refProp, *plan);
        appendProjection(refProp, *plan);
        std::vector<common::LogicalType>& refColumnTypes = columnTypes;
        auto copyTo = std::make_shared<LogicalCopyTo>(filePath + "/" + schema->tableName + ".csv",
            fileType, columnNames, refColumnTypes, boundExportDatabase.getCopyOption()->copy(),
            plan->getLastOperator());
        copyTo->computeFactorizedSchema();
        logicalOperators.push_back(std::move(copyTo));
    }

    // create rel copyTO operator
    for (auto& schema : catalog->getRelTableSchemas(&DUMMY_READ_TRANSACTION)) {
        auto tableName = schema->tableName;
        std::vector<std::string> columnNames;
        std::vector<common::LogicalType> columnTypes;
        auto expProperties = std::make_shared<binder::expression_vector>();
        getColumnInfo(columnTypes, columnNames, schema);
        getProp(expProperties, schema);
        std::vector<common::LogicalType>& refColumnTypes = columnTypes;
        auto expandLogical = appendRelForExportDB(schema, catalog);
        auto copyTo = std::make_shared<LogicalCopyTo>(filePath + "/" + tableName + ".csv", fileType,
            columnNames, refColumnTypes, boundExportDatabase.getCopyOption()->copy(),
            expandLogical);
        copyTo->computeFactorizedSchema();
        logicalOperators.push_back(std::move(copyTo));
    }
    auto tableCyphers = getTableCypher(catalog);
    auto tableNames = getTableNames(catalog);
    auto macroCyphers = getMacroCypher(catalog);
    auto exportDatabase = make_shared<LogicalExportDatabase>(filePath, std::move(tableNames),
        std::move(tableCyphers), std::move(macroCyphers), std::move(logicalOperators));
    plan->setLastOperator(std::move(exportDatabase));
    return plan;
}

} // namespace planner
} // namespace kuzu
