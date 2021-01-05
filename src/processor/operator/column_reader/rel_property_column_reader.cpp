#include "src/processor/include/operator/column_reader/rel_property_column_reader.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

RelPropertyColumnReader::RelPropertyColumnReader(FileDeserHelper& fdsh)
    : ColumnReader{fdsh}, propertyName{*fdsh.readString()}, relLabel{fdsh.read<label_t>()} {
    this->setPrevOperator(deserializeOperator(fdsh));
};

void RelPropertyColumnReader::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getRelPropertyColumn(relLabel, nodeLabel, propertyName);
    auto name = nodeOrRelVarName + "." + propertyName;
    outValueVector = make_shared<ValueVector>(name, column->getElementSize());
    inDataChunk->append(outValueVector);
}

void RelPropertyColumnReader::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(RelPropertyColumnReader).name();
    fsh.writeString(typeIDStr);
    ColumnReader::serialize(fsh);
    fsh.writeString(propertyName);
    fsh.write(relLabel);
    Operator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
