#pragma once

#include "processor/operator/base_extend.h"
#include "processor/operator/filtering_operator.h"
#include "storage/storage_structure/column.h"

namespace kuzu {
namespace processor {

class ColumnExtendAndScanRelProperties : public BaseExtendAndScanRelProperties,
                                         public FilteringOperator {
public:
    ColumnExtendAndScanRelProperties(const DataPos& inNodeIDVectorPos,
        const DataPos& outNodeIDVectorPos, vector<DataPos> outPropertyVectorsPos, Column* adjColumn,
        vector<Column*> propertyColumns, unique_ptr<PhysicalOperator> child, uint32_t id,
        const string& paramsString)
        : BaseExtendAndScanRelProperties{PhysicalOperatorType::COLUMN_EXTEND, inNodeIDVectorPos,
              outNodeIDVectorPos, std::move(outPropertyVectorsPos), std::move(child), id,
              paramsString},
          FilteringOperator{1 /* numStatesToSave */}, adjColumn{adjColumn},
          propertyColumns{std::move(propertyColumns)} {}
    ~ColumnExtendAndScanRelProperties() override = default;

    bool getNextTuplesInternal() override;

    inline unique_ptr<PhysicalOperator> clone() override {
        return make_unique<ColumnExtendAndScanRelProperties>(inNodeIDVectorPos, outNodeIDVectorPos,
            outPropertyVectorsPos, adjColumn, propertyColumns, children[0]->clone(), id,
            paramsString);
    }

private:
    Column* adjColumn;
    vector<Column*> propertyColumns;
};

} // namespace processor
} // namespace kuzu
