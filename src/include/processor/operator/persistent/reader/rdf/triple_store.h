#pragma once

#include <string>
#include <vector>

#include "common/copy_constructors.h"

namespace kuzu {
namespace processor {

struct TripleStore {
    std::vector<std::string> subjects;
    std::vector<std::string> predicates;
    std::vector<std::string> objects;

    virtual ~TripleStore() = default;
};

struct LiteralTripleStore : public TripleStore {
    std::vector<std::string> objectType;
};

struct RdfStore {
    TripleStore resourceTripleStore;
    LiteralTripleStore literalTripleStore;

    RdfStore() : resourceTripleStore{}, literalTripleStore{} {}
    DELETE_COPY_CONSTRUCT(RdfStore);
};

} // namespace processor
} // namespace kuzu
