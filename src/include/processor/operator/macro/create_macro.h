#pragma once

#include "catalog/catalog.h"
#include "function/scalar_macro_function.h"
#include "processor/operator/physical_operator.h"

namespace kuzu {
namespace processor {

struct CreateMacroInfo {
    std::string macroName;
    std::unique_ptr<function::ScalarMacroFunction> macro;
    DataPos outputPos;
    catalog::Catalog* catalog;

    CreateMacroInfo(std::string macroName, std::unique_ptr<function::ScalarMacroFunction> macro,
        DataPos outputPos, catalog::Catalog* catalog)
        : macroName{std::move(macroName)}, macro{std::move(macro)}, outputPos{std::move(outputPos)},
          catalog{catalog} {}

    inline std::unique_ptr<CreateMacroInfo> copy() {
        return std::make_unique<CreateMacroInfo>(macroName, macro->copy(), outputPos, catalog);
    }
};

class CreateMacro : public PhysicalOperator {
public:
    CreateMacro(PhysicalOperatorType operatorType, std::unique_ptr<CreateMacroInfo> createMacroInfo,
        uint32_t id, const std::string& paramsString)
        : PhysicalOperator{operatorType, id, paramsString}, createMacroInfo{
                                                                std::move(createMacroInfo)} {}

    inline bool isSource() const override { return true; }
    inline bool canParallel() const final { return false; }

    inline void initLocalStateInternal(
        ResultSet* resultSet, ExecutionContext* /*context*/) override {
        outputVector = resultSet->getValueVector(createMacroInfo->outputPos).get();
    }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<CreateMacro>(
            operatorType, createMacroInfo->copy(), id, paramsString);
    }

private:
    std::unique_ptr<CreateMacroInfo> createMacroInfo;
    bool hasExecuted = false;
    common::ValueVector* outputVector;
};

} // namespace processor
} // namespace kuzu
