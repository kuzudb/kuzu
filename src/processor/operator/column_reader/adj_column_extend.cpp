#include "src/processor/include/operator/column_reader/adj_column_extend.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

AdjColumnExtend::AdjColumnExtend(FileDeserHelper& fdsh)
    : ColumnReader{fdsh}, nbrNodeVarName{*fdsh.readString()}, direction{fdsh.read<Direction>()},
      relLabel{fdsh.read<label_t>()} {
    this->setPrevOperator(deserializeOperator(fdsh));
};

void AdjColumnExtend::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getAdjColumn(direction, nodeLabel, relLabel);
    auto outNodeIDVector =
        make_shared<NodeIDVector>(nbrNodeVarName, ((AdjColumn*)column)->getCompressionScheme());
    outNodeIDVector->setIsSequence(inNodeIDVector->getIsSequence());
    outValueVector = static_pointer_cast<ValueVector>(outNodeIDVector);
    inDataChunk->append(outValueVector);
    dataChunks->append(inDataChunk);
}

void AdjColumnExtend::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(AdjColumnExtend).name();
    fsh.writeString(typeIDStr);
    ColumnReader::serialize(fsh);
    fsh.writeString(nbrNodeVarName);
    fsh.write(direction);
    fsh.write(relLabel);
    Operator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
