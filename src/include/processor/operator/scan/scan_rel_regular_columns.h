#pragma once

#include "processor/operator/filtering_operator.h"
#include "processor/operator/scan/scan_rel_table.h"

namespace kuzu {
namespace processor {

class ScanRelRegularColumns : public ScanRelTable, SelVectorOverWriter {
public:
    ScanRelRegularColumns(std::unique_ptr<ScanRelTableInfo> info, const DataPos& inVectorPos,
        std::vector<DataPos> outVectorsPos, std::unique_ptr<PhysicalOperator> child, uint32_t id,
        const std::string& paramsString)
        : ScanRelTable{
              std::move(info), inVectorPos, outVectorsPos, std::move(child), id, paramsString} {}

    bool getNextTuplesInternal(ExecutionContext* context) final;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ScanRelRegularColumns>(
            info->copy(), inVectorPos, outVectorsPos, children[0]->clone(), id, paramsString);
    }
};

} // namespace processor
} // namespace kuzu
