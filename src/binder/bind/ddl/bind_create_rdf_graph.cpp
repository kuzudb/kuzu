#include "binder/binder.h"
#include "binder/ddl/bound_create_table_info.h"
#include "catalog/catalog.h"
#include "catalog/catalog_entry/rdf_graph_catalog_entry.h"
#include "common/keyword/rdf_keyword.h"
#include "function/sequence/sequence_functions.h"
#include "parser/ddl/create_table_info.h"
#include "parser/expression/parsed_function_expression.h"
#include "parser/expression/parsed_literal_expression.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static std::unique_ptr<parser::ParsedFunctionExpression> createSerialDefaultExpr(std::string name) {
    auto param = std::make_unique<parser::ParsedLiteralExpression>(Value(name), "");
    return std::make_unique<parser::ParsedFunctionExpression>(function::NextValFunction::name,
        std::move(param), "");
}

BoundCreateTableInfo Binder::bindCreateRdfGraphInfo(const CreateTableInfo* info) {
    auto rdfGraphName = info->tableName;
    // Resource table.
    auto resourceTableName = RDFGraphCatalogEntry::getResourceTableName(rdfGraphName);
    std::vector<PropertyInfo> resourceProperties;
    resourceProperties.emplace_back(std::string(rdf::IRI), LogicalType::STRING());
    auto resourceExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(0 /* primaryKeyIdx */,
        std::move(resourceProperties));
    auto resourceCreateInfo = BoundCreateTableInfo(TableType::NODE, resourceTableName,
        info->onConflict, std::move(resourceExtraInfo));
    // Literal table.
    auto literalTableName = RDFGraphCatalogEntry::getLiteralTableName(rdfGraphName);
    std::vector<PropertyInfo> literalProperties;
    literalProperties.emplace_back(std::string(rdf::ID), LogicalType::SERIAL(),
        createSerialDefaultExpr(Catalog::genSerialName(literalTableName, std::string(rdf::ID))));
    literalProperties.emplace_back(std::string(rdf::VAL), LogicalType::RDF_VARIANT());
    literalProperties.emplace_back(std::string(rdf::LANG), LogicalType::STRING());
    auto literalExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(0 /* primaryKeyIdx */,
        std::move(literalProperties));
    auto literalCreateInfo = BoundCreateTableInfo(TableType::NODE, literalTableName,
        info->onConflict, std::move(literalExtraInfo));
    // Resource triple table.
    auto resourceTripleTableName = RDFGraphCatalogEntry::getResourceTripleTableName(rdfGraphName);
    std::vector<PropertyInfo> resourceTripleProperties;
    resourceTripleProperties.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    resourceTripleProperties.emplace_back(std::string(rdf::PID), LogicalType::INTERNAL_ID());
    auto boundResourceTripleExtraInfo =
        std::make_unique<BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY, RelMultiplicity::MANY,
            INVALID_TABLE_ID, INVALID_TABLE_ID, std::move(resourceTripleProperties));
    auto boundResourceTripleCreateInfo = BoundCreateTableInfo(TableType::REL,
        resourceTripleTableName, info->onConflict, std::move(boundResourceTripleExtraInfo));
    // Literal triple table.
    auto literalTripleTableName = RDFGraphCatalogEntry::getLiteralTripleTableName(rdfGraphName);
    std::vector<PropertyInfo> literalTripleProperties;
    literalTripleProperties.emplace_back(InternalKeyword::ID, LogicalType::INTERNAL_ID());
    literalTripleProperties.emplace_back(std::string(rdf::PID), LogicalType::INTERNAL_ID());
    auto boundLiteralTripleExtraInfo =
        std::make_unique<BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY, RelMultiplicity::MANY,
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
