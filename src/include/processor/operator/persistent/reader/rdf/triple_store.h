#pragma once

#include <string>
#include <vector>

#include "common/assert.h"
#include "common/types/types.h"

namespace kuzu {
namespace processor {

struct RdfStore {
    virtual ~RdfStore() = default;

    virtual bool isEmpty() const = 0;
    virtual uint64_t size() const = 0;
    virtual void clear() = 0;
};

struct ResourceStore final : public RdfStore {
    std::vector<std::string> resources;

    inline bool isEmpty() const override { return resources.empty(); }

    inline uint64_t size() const override { return resources.size(); }

    void clear() override { resources.clear(); }
};

struct LiteralStore final : public RdfStore {
    std::vector<std::string> literals;
    std::vector<common::LogicalTypeID> literalTypes;
    std::vector<std::string> langs;

    inline bool isEmpty() const override { return literals.empty(); }

    inline uint64_t size() const override { return literals.size(); }

    inline void clear() override {
        literals.clear();
        literalTypes.clear();
        langs.clear();
    }
};

struct ResourceTripleStore final : public RdfStore {
    std::vector<std::string> subjects;
    std::vector<std::string> predicates;
    std::vector<std::string> objects;

    inline bool isEmpty() const override { return subjects.empty(); }

    inline uint64_t size() const override { return subjects.size(); }

    inline void clear() override {
        subjects.clear();
        predicates.clear();
        objects.clear();
    }
};

struct LiteralTripleStore final : public RdfStore {
    std::vector<std::string> subjects;
    std::vector<std::string> predicates;
    std::vector<std::string> objects;
    std::vector<common::LogicalTypeID> objectTypes;
    std::vector<std::string> langs;

    inline bool isEmpty() const override { return subjects.empty(); }

    inline uint64_t size() const override { return subjects.size(); }

    inline void clear() override {
        subjects.clear();
        predicates.clear();
        objects.clear();
        objectTypes.clear();
        langs.clear();
    }
};

struct TripleStore final : public RdfStore {
    ResourceTripleStore rtStore;
    LiteralTripleStore ltStore;

    bool isEmpty() const override { KU_UNREACHABLE; }

    inline uint64_t size() const override { KU_UNREACHABLE; }

    inline void clear() override { KU_UNREACHABLE; }
};

} // namespace processor
} // namespace kuzu
