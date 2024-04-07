#pragma once

#include <string_view>

namespace kuzu {
namespace common {
namespace rdf {

static constexpr std::string_view IRI = "iri";   // Resource iri
static constexpr std::string_view VAL = "val";   // Literal value
static constexpr std::string_view LANG = "lang"; // Literal
static constexpr std::string_view ID = "id";     // Literal id
static constexpr std::string_view PID = "pid";   // Predicate resource id
static constexpr std::string_view SUBJECT = "subject";
static constexpr std::string_view PREDICATE = "predicate";
static constexpr std::string_view OBJECT = "object";
static constexpr std::string_view RESOURCE_TABLE_SUFFIX = "_r";
static constexpr std::string_view LITERAL_TABLE_SUFFIX = "_l";
static constexpr std::string_view RESOURCE_TRIPLE_TABLE_SUFFIX = "_rt";
static constexpr std::string_view LITERAL_TRIPLE_TABLE_SUFFIX = "_lt";

// Common RDF prefix
static constexpr std::string_view XSD = "http://www.w3.org/2001/XMLSchema#";
static constexpr std::string_view XSD_integer = "integer";
static constexpr std::string_view XSD_double = "double";
static constexpr std::string_view XSD_decimal = "decimal";
static constexpr std::string_view XSD_boolean = "boolean";
static constexpr std::string_view XSD_date = "date";
static constexpr std::string_view XSD_dateTime = "dateTime";
static constexpr std::string_view XSD_nonNegativeInteger = "nonNegativeInteger";
static constexpr std::string_view XSD_positiveInteger = "positiveInteger";
static constexpr std::string_view XSD_float = "float";

} // namespace rdf
} // namespace common
} // namespace kuzu
