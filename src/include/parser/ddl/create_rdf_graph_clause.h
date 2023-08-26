#pragma once

#include "parser/ddl/ddl.h"

namespace kuzu {
namespace parser {

class CreateRDFGraphClause : public DDL {
public:
    explicit CreateRDFGraphClause(std::string rdfGraphName)
        : DDL{common::StatementType::CREATE_RDF_GRAPH, std::move(rdfGraphName)} {}
};

} // namespace parser
} // namespace kuzu
