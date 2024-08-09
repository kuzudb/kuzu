#include "binder/binder.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/keyword/rdf_keyword.h"
#include "parser/ddl/create_table_info.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

BoundCreateTableInfo Binder::bindCreateRdfGraphInfo(const CreateTableInfo* info) {
    auto rdfGraphName = info->tableName;
    // Resource table.
    auto resourceTableName = RDFGraphCatalogEntry::getResourceTableName(rdfGraphName);
    auto iriColumnDefinition = ColumnDefinition(std::string(rdf::IRI), LogicalType::STRING());
    std::vector<PropertyDefinition> resourceProperties;
    resourceProperties.emplace_back(iriColumnDefinition.copy());
    auto resourceExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(
        iriColumnDefinition.name, std::move(resourceProperties));
    auto resourceCreateInfo = BoundCreateTableInfo(TableType::NODE, resourceTableName,
        info->onConflict, std::move(resourceExtraInfo));
    // Literal table.
    auto literalTableName = RDFGraphCatalogEntry::getLiteralTableName(rdfGraphName);
    auto literalIDColumnDefinition = ColumnDefinition(std::string(rdf::ID), LogicalType::SERIAL());
    auto literalValColumnDefinition =
        ColumnDefinition(std::string(rdf::VAL), LogicalType::RDF_VARIANT());
    auto literalLangColumnDefinition =
        ColumnDefinition(std::string(rdf::LANG), LogicalType::STRING());
    std::vector<PropertyDefinition> literalProperties;
    auto literalIDDefault = ParsedExpressionUtils::getSerialDefaultExpr(
        Catalog::genSerialName(literalTableName, std::string(rdf::ID)));
    literalProperties.emplace_back(literalIDColumnDefinition.copy(), std::move(literalIDDefault));
    literalProperties.emplace_back(literalValColumnDefinition.copy());
    literalProperties.emplace_back(literalLangColumnDefinition.copy());
    auto literalExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(
        literalIDColumnDefinition.name, std::move(literalProperties));
    auto literalCreateInfo = BoundCreateTableInfo(TableType::NODE, literalTableName,
        info->onConflict, std::move(literalExtraInfo));
    // Resource triple table.
    auto resourceTripleTableName = RDFGraphCatalogEntry::getResourceTripleTableName(rdfGraphName);
    auto resourceTripleIDColumnDefinition =
        ColumnDefinition(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    auto resourceTriplePIDColumnDefinition =
        ColumnDefinition(std::string(rdf::PID), LogicalType::INTERNAL_ID());
    std::vector<PropertyDefinition> resourceTripleProperties;
    resourceTripleProperties.emplace_back(resourceTripleIDColumnDefinition.copy());
    resourceTripleProperties.emplace_back(resourceTriplePIDColumnDefinition.copy());
    auto boundResourceTripleExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(
        INVALID_TABLE_ID, INVALID_TABLE_ID, std::move(resourceTripleProperties));
    auto boundResourceTripleCreateInfo = BoundCreateTableInfo(TableType::REL,
        resourceTripleTableName, info->onConflict, std::move(boundResourceTripleExtraInfo));
    // Literal triple table.
    auto literalTripleTableName = RDFGraphCatalogEntry::getLiteralTripleTableName(rdfGraphName);
    auto literalTripleIDColumnDefinition =
        ColumnDefinition(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    auto literalTriplePIDColumnDefinition =
        ColumnDefinition(std::string(rdf::PID), LogicalType::INTERNAL_ID());
    std::vector<PropertyDefinition> literalTripleProperties;
    literalTripleProperties.emplace_back(literalTripleIDColumnDefinition.copy());
    literalTripleProperties.emplace_back(literalTriplePIDColumnDefinition.copy());
    auto boundLiteralTripleExtraInfo = std::make_unique<BoundExtraCreateRelTableInfo>(
        INVALID_TABLE_ID, INVALID_TABLE_ID, std::move(literalTripleProperties));
    auto boundLiteralTripleCreateInfo = BoundCreateTableInfo(TableType::REL, literalTripleTableName,
        info->onConflict, std::move(boundLiteralTripleExtraInfo));
    // Rdf table.
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRdfGraphInfo>(
        std::move(resourceCreateInfo), std::move(literalCreateInfo),
        std::move(boundResourceTripleCreateInfo), std::move(boundLiteralTripleCreateInfo));
    return BoundCreateTableInfo(TableType::RDF, rdfGraphName, info->onConflict,
        std::move(boundExtraInfo));
}

} // namespace binder
} // namespace kuzu
