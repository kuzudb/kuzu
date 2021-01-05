#include "src/processor/include/operator/column_reader/node_property_column_reader.h"

#include "src/processor/include/operator/operator_ser_deser_factory.h"

namespace graphflow {
namespace processor {

NodePropertyColumnReader::NodePropertyColumnReader(FileDeserHelper& fdsh)
    : ColumnReader{fdsh}, propertyName{*fdsh.readString()} {
    this->setPrevOperator(deserializeOperator(fdsh));
}

void NodePropertyColumnReader::initialize(Graph* graph) {
    ColumnReader::initialize(graph);
    column = graph->getNodePropertyColumn(nodeLabel, propertyName);
    auto name = nodeOrRelVarName + "." + propertyName;
    outValueVector = make_shared<ValueVector>(name, column->getElementSize());
    inDataChunk->append(outValueVector);
}

void NodePropertyColumnReader::serialize(FileSerHelper& fsh) {
    string typeIDStr = typeid(NodePropertyColumnReader).name();
    fsh.writeString(typeIDStr);
    ColumnReader::serialize(fsh);
    fsh.writeString(propertyName);
    Operator::serialize(fsh);
}

} // namespace processor
} // namespace graphflow
