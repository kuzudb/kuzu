#pragma once

#include <string_view>

namespace kuzu {
namespace common {
namespace rdf {

static constexpr std::string_view IRI = "iri";
static constexpr std::string_view ID = "id";
static constexpr std::string_view PID = "pid"; // TODO: rename
static constexpr std::string_view SUBJECT = "subject";
static constexpr std::string_view PREDICATE = "predicate";
static constexpr std::string_view OBJECT = "object";
static constexpr std::string_view SUBJECT_OFFSET = "subject_offset";
static constexpr std::string_view PREDICATE_OFFSET = "predicate_offset";
static constexpr std::string_view OBJECT_OFFSET = "object_offset";
static constexpr std::string_view RESOURCE_TABLE_SUFFIX = "_resource_t";
static constexpr std::string_view LITERAL_TABLE_SUFFIX = "_literal_t";
static constexpr std::string_view RESOURCE_TRIPLE_TABLE_SUFFIX = "_resource_triples_t";
static constexpr std::string_view LITERAL_TRIPLE_TABLE_SUFFIX = "_literal_triples_t";

// Common RDF prefix
static constexpr std::string_view XSD = "http://www.w3.org/2001/XMLSchema#";
static constexpr std::string_view XSD_integer = "integer";
static constexpr std::string_view XSD_double = "double";
static constexpr std::string_view XSD_decimal = "decimal";
static constexpr std::string_view XSD_boolean = "boolean";
static constexpr std::string_view XSD_date = "date";

} // namespace rdf
} // namespace common
} // namespace kuzu
