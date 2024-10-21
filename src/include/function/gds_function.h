#pragma once

#include "function.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

struct GDSFunction : public Function {
    std::unique_ptr<GDSAlgorithm> gds;

    GDSFunction() = default;
    GDSFunction(std::string name, std::vector<common::LogicalTypeID> parameterTypeIDs,
        std::unique_ptr<GDSAlgorithm> gds)
        : Function{std::move(name), std::move(parameterTypeIDs)}, gds{std::move(gds)} {}
    EXPLICIT_COPY_DEFAULT_MOVE(GDSFunction);

private:
    GDSFunction(const GDSFunction& other) : Function{other}, gds{other.gds->copy()} {}
};

} // namespace function
} // namespace kuzu
