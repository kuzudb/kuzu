#include "binder/binder.h"
#include "catalog/rel_table_schema.h"
#include "common/keyword/rdf_keyword.h"
#include "common/types/rdf_variant_type.h"
#include "parser/ddl/create_table_info.h"

using namespace kuzu::parser;
using namespace kuzu::common;
using namespace kuzu::catalog;

namespace kuzu {
namespace binder {

static std::string getRdfResourceTableName(const std::string& rdfName) {
    return rdfName + std::string(rdf::RESOURCE_TABLE_SUFFIX);
}

static std::string getRdfLiteralTableName(const std::string& rdfName) {
    return rdfName + std::string(rdf::LITERAL_TABLE_SUFFIX);
}

static inline std::string getRdfResourceTripleTableName(const std::string& rdfName) {
    return rdfName + std::string(rdf::RESOURCE_TRIPLE_TABLE_SUFFIX);
}

static std::string getRdfLiteralTripleTableName(const std::string& rdfName) {
    return rdfName + std::string(rdf::LITERAL_TRIPLE_TABLE_SUFFIX);
}

std::unique_ptr<BoundCreateTableInfo> Binder::bindCreateRdfGraphInfo(const CreateTableInfo* info) {
    auto rdfGraphName = info->tableName;
    auto stringType = LogicalType(LogicalTypeID::STRING);
    auto serialType = LogicalType(LogicalTypeID::SERIAL);
    // Resource table.
    auto resourceTableName = getRdfResourceTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> resourceProperties;
    resourceProperties.push_back(
        std::make_unique<Property>(std::string(rdf::IRI), stringType.copy()));
    auto resourceExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(
        0 /* primaryKeyIdx */, std::move(resourceProperties));
    auto resourceCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::NODE, resourceTableName, std::move(resourceExtraInfo));
    // Literal table.
    auto literalTableName = getRdfLiteralTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> literalProperties;
    literalProperties.push_back(
        std::make_unique<Property>(std::string(rdf::ID), serialType.copy()));
    literalProperties.push_back(
        std::make_unique<Property>(std::string(rdf::IRI), RdfVariantType::getType()));
    auto literalExtraInfo = std::make_unique<BoundExtraCreateNodeTableInfo>(
        0 /* primaryKeyIdx */, std::move(literalProperties));
    auto literalCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::NODE, literalTableName, std::move(literalExtraInfo));
    // Resource triple table.
    auto resourceTripleTableName = getRdfResourceTripleTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> resourceTripleProperties;
    resourceTripleProperties.push_back(
        std::make_unique<Property>(std::string(rdf::PID), LogicalType::INTERNAL_ID()));
    auto boundResourceTripleExtraInfo =
        std::make_unique<BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY_MANY, INVALID_TABLE_ID,
            INVALID_TABLE_ID, std::move(resourceTripleProperties));
    auto boundResourceTripleCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::REL, resourceTripleTableName, std::move(boundResourceTripleExtraInfo));
    // Literal triple table.
    auto literalTripleTableName = getRdfLiteralTripleTableName(rdfGraphName);
    std::vector<std::unique_ptr<Property>> literalTripleProperties;
    literalTripleProperties.push_back(
        std::make_unique<Property>(std::string(rdf::PID), LogicalType::INTERNAL_ID()));
    auto boundLiteralTripleExtraInfo =
        std::make_unique<BoundExtraCreateRelTableInfo>(RelMultiplicity::MANY_MANY, INVALID_TABLE_ID,
            INVALID_TABLE_ID, std::move(literalTripleProperties));
    auto boundLiteralTripleCreateInfo = std::make_unique<BoundCreateTableInfo>(
        TableType::REL, literalTripleTableName, std::move(boundLiteralTripleExtraInfo));
    // Rdf table.
    auto boundExtraInfo = std::make_unique<BoundExtraCreateRdfGraphInfo>(
        std::move(resourceCreateInfo), std::move(literalCreateInfo),
        std::move(boundResourceTripleCreateInfo), std::move(boundLiteralTripleCreateInfo));
    return std::make_unique<BoundCreateTableInfo>(
        TableType::RDF, rdfGraphName, std::move(boundExtraInfo));
}

} // namespace binder
} // namespace kuzu
