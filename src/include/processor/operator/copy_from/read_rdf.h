#pragma once

#include <memory>
#include <utility>

#include "common/types/internal_id_t.h"
#include "common/vector/value_vector.h"
#include "processor/data_pos.h"
#include "processor/operator/physical_operator.h"
#include "storage/copier/rdf_reader.h"

namespace kuzu {
namespace processor {

struct ReadRDFMorsel {
    ReadRDFMorsel(offset_t startOffset, offset_t endOffset)
        : startOffset{startOffset}, endOffset{endOffset} {};

    offset_t startOffset;
    offset_t endOffset;
};

class ReadRDFSharedState {
public:
    static constexpr offset_t APPROX_IRI_BYTES = 60;
    explicit ReadRDFSharedState(const std::string& filePath)
        : currentOffset{0}, rdfReader{std::make_unique<storage::RDFReader>(filePath, false)} {};

    std::unique_ptr<ReadRDFMorsel> getMorsel(
        const std::shared_ptr<common::ValueVector>& subjectVector,
        const std::shared_ptr<common::ValueVector>& predicateVector,
        const std::shared_ptr<common::ValueVector>& objectVector);

    void reset();

    offset_t getApproxNumberOfIRIs();

protected:
    std::mutex mtx;
    std::unique_ptr<storage::RDFReader> rdfReader;
    offset_t currentOffset;
};

class ReadRDFFile : public PhysicalOperator {
public:
    ReadRDFFile(const DataPos& subjectVectorPos, const DataPos& predicateVectorPos,
        const DataPos& objectVectorPos, const DataPos& offsetVectorPos,
        std::shared_ptr<ReadRDFSharedState> sharedState, uint32_t id,
        const std::string& paramsString)
        : PhysicalOperator{PhysicalOperatorType::READ_RDF, id, paramsString},
          subjectVectorPos{subjectVectorPos}, predicateVectorPos{predicateVectorPos},
          objectVectorPos{objectVectorPos}, offsetVectorPos{offsetVectorPos}, sharedState{std::move(
                                                                                  sharedState)} {};

    inline void initLocalStateInternal(ResultSet* resultSet, ExecutionContext* context) override {
        subjectVector = resultSet->getValueVector(subjectVectorPos);
        predicateVector = resultSet->getValueVector(predicateVectorPos);
        objectVector = resultSet->getValueVector(objectVectorPos);
        offsetVector = resultSet->getValueVector(offsetVectorPos);
    }

    inline bool isSource() const override { return true; }

    bool getNextTuplesInternal(ExecutionContext* context) override;

    inline std::unique_ptr<PhysicalOperator> clone() override {
        return std::make_unique<ReadRDFFile>(subjectVectorPos, predicateVectorPos, objectVectorPos,
            offsetVectorPos, sharedState, id, paramsString);
    }

private:
    std::shared_ptr<ReadRDFSharedState> sharedState;
    DataPos subjectVectorPos;
    DataPos predicateVectorPos;
    DataPos objectVectorPos;
    DataPos offsetVectorPos;
    std::shared_ptr<common::ValueVector> subjectVector;
    std::shared_ptr<common::ValueVector> predicateVector;
    std::shared_ptr<common::ValueVector> objectVector;
    std::shared_ptr<common::ValueVector> offsetVector;
};

} // namespace processor
} // namespace kuzu
