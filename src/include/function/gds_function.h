#pragma once

#include "function.h"
#include "function/gds/gds.h"

namespace kuzu {
namespace function {

struct GDSFunction : public Function {
    std::unique_ptr<GDSAlgorithm> gds;

    GDSFunction(std::string name, std::unique_ptr<GDSAlgorithm> gds)
        : Function{std::move(name), gds->getParameterTypeIDs()}, gds{std::move(gds)} {}
    GDSFunction(const GDSFunction& other) : Function{other}, gds{other.gds->copy()} {}

    std::unique_ptr<Function> copy() const override { return std::make_unique<GDSFunction>(*this); }
};

} // namespace function
} // namespace kuzu
