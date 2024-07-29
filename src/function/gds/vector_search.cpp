#include <queue>

#include "binder/binder.h"
#include "binder/expression/expression_util.h"
#include "common/types/value/nested.h"
#include "common/vector_index/helpers.h"
#include "function/gds/gds.h"
#include "function/gds/gds_function_collection.h"
#include "function/gds_function.h"
#include "graph/graph.h"
#include "main/client_context.h"
#include "processor/operator/gds_call.h"
#include "processor/result/factorized_table.h"
#include "storage/index/vector_index_header.h"
#include "storage/storage_manager.h"
#include "storage/store/column.h"
#include "storage/store/node_table.h"

using namespace kuzu::processor;
using namespace kuzu::common;
using namespace kuzu::binder;
using namespace kuzu::storage;
using namespace kuzu::graph;

namespace kuzu {
namespace function {

struct VectorSearchBindData final : public GDSBindData {
    table_id_t nodeTableID = INVALID_TABLE_ID;
    property_id_t embeddingPropertyID = INVALID_PROPERTY_ID;
    int k = 100;
    int efSearch = 100;
    std::vector<float> queryVector;

    VectorSearchBindData(std::shared_ptr<binder::Expression> nodeOutput, table_id_t nodeTableID,
        property_id_t embeddingPropertyID, int k, int64_t efSearch, std::vector<float> queryVector,
        bool outputAsNode)
        : GDSBindData{std::move(nodeOutput), outputAsNode}, nodeTableID(nodeTableID),
          embeddingPropertyID(embeddingPropertyID), k(k), efSearch(efSearch),
          queryVector(std::move(queryVector)){};

    VectorSearchBindData(const VectorSearchBindData& other)
        : GDSBindData{other}, nodeTableID(other.nodeTableID),
          embeddingPropertyID(other.embeddingPropertyID), k(other.k), efSearch{other.efSearch},
          queryVector(other.queryVector) {}

    std::unique_ptr<GDSBindData> copy() const override {
        return std::make_unique<VectorSearchBindData>(*this);
    }
};

class VectorSearchLocalState : public GDSLocalState {
public:
    explicit VectorSearchLocalState(main::ClientContext* context, table_id_t nodeTableId,
        property_id_t embeddingPropertyId) {
        auto mm = context->getMemoryManager();
        nodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID(), mm);
        distanceVector = std::make_unique<ValueVector>(LogicalType::DOUBLE(), mm);
        nodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
        distanceVector->state = DataChunkState::getSingleValueDataChunkState();
        vectors.push_back(nodeIdVector.get());
        vectors.push_back(distanceVector.get());

        indexHeader = context->getStorageManager()->getVectorIndexHeaderReadOnlyVersion(nodeTableId,
            embeddingPropertyId);
        auto nodeTable = ku_dynamic_cast<Table*, NodeTable*>(
            context->getStorageManager()->getTable(nodeTableId));
        embeddingColumn = nodeTable->getColumn(embeddingPropertyId);
        compressedColumn = nodeTable->getColumn(indexHeader->getCompressedPropertyId());

        // TODO: Replace with compressed vector. Should be stored in the
        embeddingVector = std::make_unique<ValueVector>(embeddingColumn->getDataType().copy(),
            context->getMemoryManager());
        embeddingVector->state = DataChunkState::getSingleValueDataChunkState();
        compressedVector = std::make_unique<ValueVector>(compressedColumn->getDataType().copy(),
            context->getMemoryManager());
        compressedVector->state = DataChunkState::getSingleValueDataChunkState();
        inputNodeIdVector = std::make_unique<ValueVector>(LogicalType::INTERNAL_ID().copy(),
            context->getMemoryManager());
        inputNodeIdVector->state = DataChunkState::getSingleValueDataChunkState();
        for (size_t i = 0; i < compressedColumn->getNumNodeGroups(context->getTx()); i++) {
            readStates.push_back(std::make_unique<Column::ChunkState>());
        }
    }

    void materialize(graph::Graph* graph, std::priority_queue<NodeDistCloser>& result,
        FactorizedTable& table, int k) const {
        // Remove elements until we have k elements
        while (result.size() > k) {
            result.pop();
        }

        std::priority_queue<NodeDistFarther> reversed;
        while (!result.empty()) {
            reversed.emplace(result.top().id, result.top().dist);
            result.pop();
        }

        while (!reversed.empty()) {
            auto res = reversed.top();
            reversed.pop();
            auto nodeID = nodeID_t{res.id, indexHeader->getNodeTableId()};
            nodeIdVector->setValue<nodeID_t>(0, nodeID);
            distanceVector->setValue<double>(0, res.dist);
            table.append(vectors);
        }
    }

public:
    std::unique_ptr<ValueVector> nodeIdVector;
    std::unique_ptr<ValueVector> distanceVector;
    std::vector<ValueVector*> vectors;

    VectorIndexHeader* indexHeader;
    Column* embeddingColumn;
    std::unique_ptr<ValueVector> embeddingVector;
    Column* compressedColumn;
    std::unique_ptr<ValueVector> compressedVector;
    std::unique_ptr<ValueVector> inputNodeIdVector;
    std::vector<std::unique_ptr<Column::ChunkState>> readStates;
};

class VectorSearch : public GDSAlgorithm {
    static constexpr char DISTANCE_COLUMN_NAME[] = "_distance";

public:
    VectorSearch() = default;
    VectorSearch(const VectorSearch& other) : GDSAlgorithm{other} {}

    /*
     * Inputs are
     *
     * embeddingProperty::ANY
     * efSearch::INT64
     * outputProperty::BOOL
     */
    std::vector<common::LogicalTypeID> getParameterTypeIDs() const override {
        return {LogicalTypeID::ANY, LogicalTypeID::INT64, LogicalTypeID::LIST, LogicalTypeID::INT64,
            LogicalTypeID::INT64, LogicalTypeID::BOOL};
    }

    /*
     * Outputs are
     *
     * nodeID::INTERNAL_ID
     * distance::DOUBLE
     */
    binder::expression_vector getResultColumns(binder::Binder* binder) const override {
        expression_vector columns;
        auto& outputNode = bindData->getNodeOutput()->constCast<NodeExpression>();
        columns.push_back(outputNode.getInternalID());
        columns.push_back(binder->createVariable(DISTANCE_COLUMN_NAME, LogicalType::DOUBLE()));
        return columns;
    }

    void bind(const expression_vector& params, Binder* binder, GraphEntry& graphEntry) override {
        auto nodeOutput = bindNodeOutput(binder, graphEntry);
        auto nodeTableId = graphEntry.nodeTableIDs[0];
        auto embeddingPropertyId = ExpressionUtil::getLiteralValue<int64_t>(*params[1]);
        //        auto val = params[2]->constCast<LiteralExpression>().getValue();
        //        KU_ASSERT(val.getDataType().getLogicalTypeID() == LogicalTypeID::LIST);
        //        auto childSize = NestedVal::getChildrenSize(&val);
        //        std::vector<float> queryVector(960);
        //        for (size_t i = 0; i < childSize; i++) {
        //            auto child = NestedVal::getChildVal(&val, i);
        //            KU_ASSERT(child->getDataType().getLogicalTypeID() == LogicalTypeID::DOUBLE);
        //            queryVector[i] = child->getValue<double>();
        //        }

        std::vector<float> queryVector = {0.0117, 0.0115, 0.0087, 0.01, 0.0785, 0.1, 0.0784, 0.053,
            0.0524, 0.0819, 0.0658, 0.058, 0.0159, 0.017, 0.0461, 0.0242, 0.0084, 0.0064, 0.0072,
            0.0102, 0.0304, 0.0679, 0.0589, 0.0571, 0.0333, 0.0786, 0.0892, 0.0423, 0.0138, 0.0133,
            0.029, 0.0219, 0.009, 0.0122, 0.0107, 0.0108, 0.0266, 0.0385, 0.0571, 0.052, 0.0355,
            0.0488, 0.0692, 0.0531, 0.0144, 0.0143, 0.0149, 0.025, 0.0171, 0.0161, 0.0106, 0.0324,
            0.0271, 0.0458, 0.0531, 0.0624, 0.0316, 0.0608, 0.0661, 0.0813, 0.0186, 0.0227, 0.0116,
            0.0337, 0.0247, 0.0118, 0.0107, 0.0639, 0.0395, 0.0403, 0.0525, 0.0958, 0.0551, 0.0676,
            0.0858, 0.1749, 0.0244, 0.0281, 0.0087, 0.0512, 0.0149, 0.0086, 0.0124, 0.0356, 0.0328,
            0.0387, 0.0463, 0.0489, 0.0492, 0.0641, 0.0705, 0.1164, 0.0206, 0.0162, 0.0125, 0.0402,
            0.0135, 0.0113, 0.0074, 0.0118, 0.033, 0.0298, 0.0365, 0.042, 0.0441, 0.0519, 0.0659,
            0.0527, 0.0139, 0.0162, 0.0151, 0.0224, 0.0118, 0.0088, 0.0078, 0.0109, 0.0373, 0.0705,
            0.0628, 0.0391, 0.0242, 0.0558, 0.0721, 0.0448, 0.0168, 0.0134, 0.0331, 0.0208, 0.0041,
            0.009, 0.0165, 0.0117, 0.0962, 0.1359, 0.1215, 0.088, 0.0639, 0.1174, 0.097, 0.0703,
            0.0126, 0.0219, 0.0746, 0.0422, 0.0037, 0.0084, 0.01, 0.0132, 0.032, 0.0613, 0.0751,
            0.0914, 0.0429, 0.0641, 0.0747, 0.0466, 0.0087, 0.0286, 0.0515, 0.0279, 0.0073, 0.0149,
            0.0225, 0.0182, 0.0181, 0.0176, 0.0862, 0.064, 0.0275, 0.029, 0.0558, 0.0605, 0.0114,
            0.0145, 0.0207, 0.0285, 0.0208, 0.0191, 0.012, 0.045, 0.0234, 0.0231, 0.0533, 0.0818,
            0.0397, 0.0243, 0.0389, 0.1082, 0.0221, 0.0303, 0.016, 0.0534, 0.0271, 0.0189, 0.0164,
            0.0881, 0.0437, 0.017, 0.0282, 0.139, 0.0601, 0.0391, 0.0371, 0.2031, 0.0362, 0.0341,
            0.0119, 0.0745, 0.0167, 0.0053, 0.0104, 0.0456, 0.0222, 0.0201, 0.0156, 0.0458, 0.0431,
            0.0402, 0.0444, 0.1346, 0.0206, 0.0192, 0.015, 0.0468, 0.0103, 0.0066, 0.012, 0.0179,
            0.0185, 0.0195, 0.044, 0.0329, 0.0298, 0.0706, 0.0955, 0.0581, 0.0097, 0.0157, 0.0319,
            0.0406, 0.0074, 0.0098, 0.0103, 0.0116, 0.0407, 0.083, 0.0675, 0.0356, 0.0352, 0.0849,
            0.074, 0.0575, 0.008, 0.0105, 0.0502, 0.0379, 0.0099, 0.0213, 0.0365, 0.0247, 0.0481,
            0.0781, 0.0942, 0.084, 0.0417, 0.0525, 0.0729, 0.0664, 0.0101, 0.0231, 0.0868, 0.0646,
            0.0338, 0.0216, 0.019, 0.037, 0.0274, 0.0446, 0.0435, 0.1009, 0.0525, 0.0677, 0.0575,
            0.0464, 0.0227, 0.0417, 0.0475, 0.024, 0.0608, 0.0334, 0.0368, 0.0763, 0.0533, 0.0219,
            0.0608, 0.1125, 0.0674, 0.0218, 0.0527, 0.1312, 0.0589, 0.0331, 0.0216, 0.0653, 0.0221,
            0.0174, 0.0307, 0.0303, 0.0273, 0.0294, 0.0628, 0.0338, 0.0401, 0.0437, 0.059, 0.0761,
            0.0286, 0.0189, 0.0418, 0.0696, 0.008, 0.0079, 0.0095, 0.0111, 0.0818, 0.1053, 0.087,
            0.0531, 0.0518, 0.0886, 0.0751, 0.0689, 0.0158, 0.0182, 0.0516, 0.0286, 0.0062, 0.0048,
            0.0068, 0.0097, 0.0337, 0.0694, 0.0721, 0.0618, 0.0365, 0.0824, 0.1105, 0.0488, 0.0139,
            0.0127, 0.0314, 0.0209, 0.0103, 0.0085, 0.01, 0.0086, 0.0292, 0.0371, 0.0693, 0.0521,
            0.0312, 0.0465, 0.0836, 0.0486, 0.0141, 0.0119, 0.0146, 0.0228, 0.016, 0.0123, 0.0101,
            0.0303, 0.0274, 0.0497, 0.0706, 0.0674, 0.03, 0.0573, 0.0547, 0.0882, 0.0157, 0.0201,
            0.0119, 0.0353, 0.0225, 0.0112, 0.0097, 0.06, 0.0373, 0.0441, 0.0594, 0.0887, 0.0494,
            0.066, 0.0827, 0.184, 0.023, 0.0241, 0.0098, 0.0501, 0.0139, 0.0064, 0.0112, 0.0338,
            0.0315, 0.0393, 0.0474, 0.0409, 0.0426, 0.066, 0.0732, 0.125, 0.0224, 0.0155, 0.0121,
            0.0373, 0.0112, 0.0063, 0.0075, 0.0112, 0.032, 0.0283, 0.048, 0.0412, 0.0419, 0.0583,
            0.0784, 0.0516, 0.0135, 0.0142, 0.0169, 0.0195, 0.0097, 0.0057, 0.0059, 0.008, 0.0382,
            0.0748, 0.0619, 0.0505, 0.0271, 0.0609, 0.0845, 0.0551, 0.0158, 0.012, 0.0369, 0.0213,
            0.0062, 0.0144, 0.0228, 0.0113, 0.104, 0.1485, 0.1449, 0.1063, 0.0659, 0.1302, 0.1167,
            0.0963, 0.0152, 0.0256, 0.0822, 0.0454, 0.0047, 0.0056, 0.011, 0.0101, 0.035, 0.0665,
            0.0954, 0.1171, 0.0459, 0.0704, 0.0955, 0.0715, 0.0097, 0.029, 0.0574, 0.0251, 0.0085,
            0.0102, 0.02, 0.0148, 0.0248, 0.0185, 0.0928, 0.0701, 0.03, 0.0338, 0.0749, 0.0749,
            0.0149, 0.0174, 0.021, 0.0332, 0.0182, 0.0122, 0.0083, 0.0419, 0.0252, 0.024, 0.0531,
            0.0762, 0.0489, 0.0247, 0.033, 0.1258, 0.0175, 0.0256, 0.0131, 0.0524, 0.0261, 0.0149,
            0.0157, 0.0861, 0.0434, 0.0242, 0.0362, 0.1396, 0.0565, 0.0427, 0.0431, 0.2136, 0.0345,
            0.0251, 0.0143, 0.0757, 0.0146, 0.006, 0.0093, 0.0411, 0.0251, 0.0187, 0.031, 0.047,
            0.0351, 0.053, 0.0541, 0.1344, 0.0167, 0.0176, 0.0149, 0.0439, 0.0092, 0.0055, 0.0136,
            0.0184, 0.0186, 0.0233, 0.0678, 0.0405, 0.0248, 0.0789, 0.1125, 0.0682, 0.009, 0.0144,
            0.0338, 0.0389, 0.0066, 0.0129, 0.0116, 0.0091, 0.0416, 0.0913, 0.0806, 0.0461, 0.0373,
            0.0956, 0.0782, 0.0602, 0.0092, 0.0132, 0.0552, 0.0419, 0.0095, 0.0188, 0.0403, 0.0282,
            0.0528, 0.0827, 0.1135, 0.1069, 0.0432, 0.0556, 0.0898, 0.0818, 0.0119, 0.029, 0.1051,
            0.0709, 0.0328, 0.0158, 0.0191, 0.0363, 0.028, 0.0451, 0.0487, 0.1089, 0.0558, 0.0726,
            0.0727, 0.0525, 0.0207, 0.046, 0.0584, 0.0255, 0.0552, 0.0263, 0.0332, 0.0733, 0.0475,
            0.0252, 0.0662, 0.112, 0.0622, 0.0339, 0.066, 0.1363, 0.0512, 0.0276, 0.0249, 0.0683,
            0.0199, 0.0159, 0.0293, 0.0286, 0.0281, 0.0352, 0.0785, 0.0371, 0.039, 0.0509, 0.0765,
            0.0821, 0.025, 0.0152, 0.0471, 0.0753, 0.0077, 0.0092, 0.0085, 0.0087, 0.0895, 0.1157,
            0.1134, 0.0741, 0.0529, 0.1015, 0.098, 0.1066, 0.0133, 0.0174, 0.0599, 0.0287, 0.0067,
            0.0081, 0.0082, 0.0077, 0.0332, 0.0759, 0.0883, 0.088, 0.0408, 0.1076, 0.1387, 0.0743,
            0.0108, 0.0129, 0.0383, 0.0214, 0.0063, 0.0075, 0.0116, 0.0074, 0.0303, 0.039, 0.083,
            0.0561, 0.0311, 0.0644, 0.1124, 0.0502, 0.0117, 0.0105, 0.0166, 0.0223, 0.0134, 0.0114,
            0.0124, 0.0274, 0.0259, 0.0529, 0.0872, 0.0576, 0.0298, 0.0676, 0.0937, 0.0814, 0.0133,
            0.0178, 0.0112, 0.0341, 0.0208, 0.0121, 0.0124, 0.0528, 0.0295, 0.0446, 0.0989, 0.0666,
            0.0437, 0.076, 0.1093, 0.1461, 0.0232, 0.0193, 0.0136, 0.0484, 0.012, 0.0075, 0.0122,
            0.0309, 0.03, 0.044, 0.0796, 0.0408, 0.0343, 0.0696, 0.0906, 0.1152, 0.0183, 0.0108,
            0.0177, 0.0365, 0.0102, 0.0057, 0.0079, 0.0101, 0.0322, 0.0309, 0.0716, 0.0523, 0.0341,
            0.0646, 0.1177, 0.0614, 0.0117, 0.0132, 0.019, 0.0203, 0.0098, 0.0066, 0.0067, 0.0076,
            0.0413, 0.0847, 0.0974, 0.077, 0.0297, 0.0705, 0.1156, 0.0871, 0.0121, 0.0123, 0.0413,
            0.0233, 0.0061, 0.0122, 0.0228, 0.011, 0.1219, 0.171, 0.1641, 0.1407, 0.0748, 0.1586,
            0.1563, 0.1508, 0.0171, 0.0343, 0.1044, 0.0531, 0.0052, 0.0062, 0.0121, 0.0115, 0.0393,
            0.0698, 0.1048, 0.1629, 0.0452, 0.0914, 0.1193, 0.1164, 0.0072, 0.034, 0.0732, 0.0309,
            0.0054, 0.0085, 0.0198, 0.0139, 0.0307, 0.0216, 0.1018, 0.0796, 0.0345, 0.0454, 0.0958,
            0.0869, 0.0096, 0.0189, 0.0264, 0.0361, 0.0147, 0.0117, 0.0084, 0.0344, 0.0325, 0.0327,
            0.055, 0.0685, 0.049, 0.0302, 0.0409, 0.1359, 0.0159, 0.0162, 0.0128, 0.0539, 0.0154,
            0.013, 0.0152, 0.0684, 0.031, 0.0289, 0.0493, 0.1177, 0.0468, 0.056, 0.0674, 0.2025,
            0.0269, 0.0158, 0.0184, 0.0677, 0.0133, 0.0097, 0.0096, 0.0332, 0.0235, 0.0247, 0.066,
            0.047, 0.0223, 0.0684, 0.0817, 0.122, 0.0107, 0.0142, 0.0186, 0.042, 0.0092, 0.0066,
            0.0155, 0.0173, 0.0241, 0.0281, 0.1046, 0.071, 0.0204, 0.0969, 0.1524, 0.0947, 0.0095,
            0.0133, 0.0394, 0.0391, 0.007, 0.0129, 0.0136, 0.0099, 0.05, 0.1057, 0.0946, 0.0848,
            0.043, 0.1177, 0.0907, 0.0817, 0.0088, 0.0182, 0.0642, 0.0486, 0.0117, 0.0227, 0.0547,
            0.0372, 0.0646, 0.0933, 0.1581, 0.1529, 0.0495, 0.0595, 0.1293, 0.1124, 0.0113, 0.0368,
            0.1317, 0.0753, 0.0268, 0.013, 0.0218, 0.0346, 0.0275, 0.0453, 0.0694, 0.1228, 0.053,
            0.0839, 0.1111, 0.0679, 0.0214, 0.0564, 0.0766, 0.0238, 0.0396, 0.0164, 0.0232, 0.0558,
            0.0389, 0.0357, 0.0815, 0.1093, 0.0478, 0.067, 0.1038, 0.1442, 0.0374, 0.0213, 0.0197,
            0.0601, 0.0142, 0.0173, 0.0328, 0.0207, 0.03, 0.0512, 0.1142, 0.0505, 0.0337, 0.0719,
            0.1242, 0.1062, 0.0234, 0.0124, 0.0603, 0.0866};
        auto k = ExpressionUtil::getLiteralValue<int64_t>(*params[3]);
        auto efSearch = ExpressionUtil::getLiteralValue<int64_t>(*params[4]);
        auto outputProperty = ExpressionUtil::getLiteralValue<bool>(*params[5]);
        bindData = std::make_unique<VectorSearchBindData>(nodeOutput, nodeTableId,
            embeddingPropertyId, k, efSearch, queryVector, outputProperty);
    }

    void initLocalState(main::ClientContext* context) override {
        auto bind = ku_dynamic_cast<GDSBindData*, VectorSearchBindData*>(bindData.get());
        localState = std::make_unique<VectorSearchLocalState>(context, bind->nodeTableID,
            bind->embeddingPropertyID);
    }

    void searchNNOnUpperLevel(ExecutionContext* context, VectorIndexHeader* header,
        const float* query, DC<float, uint8_t>* dc, vector_id_t& nearest, double& nearestDist) {
        while (true) {
            vector_id_t prev_nearest = nearest;
            size_t begin, end;
            auto neighbors = header->getNeighbors(nearest, begin, end);
            for (size_t i = begin; i < end; i++) {
                vector_id_t neighbor = neighbors[i];
                if (neighbor == INVALID_VECTOR_ID) {
                    break;
                }
                double dist;
                auto embed = getCompressedEmbedding(context, header->getActualId(neighbor));
                dc->compute_distance(query, embed, &dist);
                if (dist < nearestDist) {
                    nearest = neighbor;
                    nearestDist = dist;
                }
            }
            if (prev_nearest == nearest) {
                break;
            }
        }
    }

    // TODO: Use in-mem compressed vectors
    // TODO: Maybe try using separate threads to separate io and computation (ideal async io)
    const uint8_t* getCompressedEmbedding(processor::ExecutionContext* context, vector_id_t id) {
        auto searchLocalState =
            ku_dynamic_cast<GDSLocalState*, VectorSearchLocalState*>(localState.get());
        auto compressedColumn = searchLocalState->compressedColumn;
        auto [nodeGroupIdx, offsetInChunk] = StorageUtils::getNodeGroupIdxAndOffsetInChunk(id);

        // Initialize the read state
        auto readState = searchLocalState->readStates[nodeGroupIdx].get();
        compressedColumn->initChunkState(context->clientContext->getTx(), nodeGroupIdx, *readState);

        // Read the embedding
        auto nodeIdVector = searchLocalState->inputNodeIdVector.get();
        nodeIdVector->setValue(0, id);
        auto resultVector = searchLocalState->compressedVector.get();
        compressedColumn->lookup(context->clientContext->getTx(), *readState, nodeIdVector,
            resultVector);
        return ListVector::getDataVector(resultVector)->getData();
    }

    void findEntrypointUsingUpperLayer(ExecutionContext* context, VectorIndexHeader* header,
        const float* query, DC<float, uint8_t>* dc, vector_id_t& entrypoint,
        double* entrypointDist) {
        uint8_t entrypointLevel;
        header->getEntrypoint(entrypoint, entrypointLevel);
        if (entrypointLevel == 1) {
            auto embedding = getCompressedEmbedding(context, header->getActualId(entrypoint));
            dc->compute_distance(query, embedding, entrypointDist);
            searchNNOnUpperLevel(context, header, query, dc, entrypoint, *entrypointDist);
            entrypoint = header->getActualId(entrypoint);
        } else {
            auto embedding = getCompressedEmbedding(context, entrypoint);
            dc->compute_distance(query, embedding, entrypointDist);
        }
    }

    void exec(ExecutionContext* context) override {
        auto searchLocalState =
            ku_dynamic_cast<GDSLocalState*, VectorSearchLocalState*>(localState.get());
        auto bindState = ku_dynamic_cast<GDSBindData*, VectorSearchBindData*>(bindData.get());
        auto header = searchLocalState->indexHeader;
        auto graph = sharedState->graph.get();
        auto nodeTableId = header->getNodeTableId();
        auto efSearch = bindState->efSearch;
        auto k = bindState->k;
        KU_ASSERT(bindState->queryVector.size() == header->getDim());
        auto query = bindState->queryVector.data();
        auto state = graph->prepareScan(header->getCSRRelTableId());
        auto dc = header->getQuantizer()->get_asym_distance_computer(L2_SQ);
        //        L2DistanceComputer dc(nullptr, header->getDim(), 0);
        //        dc.setQuery(query);
        // Todo: Use bitset here
        auto visited = std::make_unique<VisitedTable>(header->getNumVectors());
        vector_id_t entrypoint;
        double entrypointDist;
        findEntrypointUsingUpperLayer(context, header, query, dc.get(), entrypoint,
            &entrypointDist);

        std::priority_queue<NodeDistFarther> candidates;
        std::priority_queue<NodeDistCloser> results;
        candidates.emplace(entrypoint, entrypointDist);
        results.emplace(entrypoint, entrypointDist);
        visited->set(entrypoint);
        while (!candidates.empty()) {
            auto candidate = candidates.top();
            if (candidate.dist > results.top().dist) {
                break;
            }
            candidates.pop();
            auto neighbors = graph->scanFwdRandom({candidate.id, nodeTableId}, *state);
            // TODO: Try batch compute distances
            for (auto neighbor : neighbors) {
                if (visited->get(neighbor.offset)) {
                    continue;
                }
                visited->set(neighbor.offset);
                double dist;
                auto embed = getCompressedEmbedding(context, neighbor.offset);
                dc->compute_distance(query, embed, &dist);
                if (results.size() < efSearch || dist < results.top().dist) {
                    candidates.emplace(neighbor.offset, dist);
                    results.emplace(neighbor.offset, dist);
                    if (results.size() > efSearch) {
                        results.pop();
                    }
                }
            }
        }
        // Materialize the results
        searchLocalState->materialize(graph, results, *sharedState->fTable, k);
    }

    std::unique_ptr<GDSAlgorithm> copy() const override {
        return std::make_unique<VectorSearch>(*this);
    }
};

function_set VectorSearchFunction::getFunctionSet() {
    function_set result;
    auto function = std::make_unique<GDSFunction>(name, std::make_unique<VectorSearch>());
    result.push_back(std::move(function));
    return result;
}

} // namespace function
} // namespace kuzu
