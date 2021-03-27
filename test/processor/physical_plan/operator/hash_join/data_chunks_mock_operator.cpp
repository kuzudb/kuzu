#include "data_chunks_mock_operator.h"

static void initializeSelectedValuesPos(uint64_t* selectedValuesPos, uint64_t numSelectedValues) {
    for (uint64_t i = 0; i < numSelectedValues; i++) {
        selectedValuesPos[i] = i;
    }
}

void ScanMockOp::getNextTuples() {
    if (numTuples < 10) {
        dataChunks->getDataChunk(1)->currPos = numTuples;
        numTuples += 1;
        return;
    }
    dataChunks->getDataChunk(0)->size = 0;
    dataChunks->getDataChunk(1)->size = 0;
    dataChunks->getDataChunk(2)->size = 0;
    dataChunks->getDataChunk(0)->numSelectedValues = 0;
    dataChunks->getDataChunk(1)->numSelectedValues = 0;
    dataChunks->getDataChunk(2)->numSelectedValues = 0;
}

void ScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
    }
    dataChunkA->append(vectorA1);
    vectorA1->setDataChunkOwner(dataChunkA);
    dataChunkA->append(vectorA2);
    vectorA2->setDataChunkOwner(dataChunkA);
    dataChunkB->append(vectorB1);
    vectorB1->setDataChunkOwner(dataChunkB);
    dataChunkB->append(vectorB2);
    vectorB2->setDataChunkOwner(dataChunkB);
    dataChunkC->append(vectorC1);
    vectorC1->setDataChunkOwner(dataChunkC);
    dataChunkC->append(vectorC2);
    vectorC2->setDataChunkOwner(dataChunkC);

    dataChunkA->size = 10;
    dataChunkB->size = 10;
    dataChunkC->size = 10;
    dataChunkA->numSelectedValues = 10;
    dataChunkB->numSelectedValues = 10;
    dataChunkC->numSelectedValues = 10;
    dataChunkA->currPos = 0;
    dataChunkB->currPos = 0;
    dataChunkC->currPos = -1;
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ProbeScanMockOp::getNextTuples() {
    if (numTuples < 4) {
        dataChunks->getDataChunk(1)->currPos = numTuples;
        numTuples += 1;
        return;
    }
    dataChunks->getDataChunk(0)->size = 0;
    dataChunks->getDataChunk(1)->size = 0;
    dataChunks->getDataChunk(2)->size = 0;
    dataChunks->getDataChunk(0)->numSelectedValues = 0;
    dataChunks->getDataChunk(1)->numSelectedValues = 0;
    dataChunks->getDataChunk(2)->numSelectedValues = 0;
}

void ProbeScanMockOp::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
    }
    dataChunkA->append(vectorA1);
    vectorA1->setDataChunkOwner(dataChunkA);
    dataChunkA->append(vectorA2);
    vectorA2->setDataChunkOwner(dataChunkA);
    dataChunkB->append(vectorB1);
    vectorB1->setDataChunkOwner(dataChunkB);
    dataChunkB->append(vectorB2);
    vectorB2->setDataChunkOwner(dataChunkB);
    dataChunkC->append(vectorC1);
    vectorC1->setDataChunkOwner(dataChunkC);
    dataChunkC->append(vectorC2);
    vectorC2->setDataChunkOwner(dataChunkC);

    dataChunkA->size = 10;
    dataChunkB->size = 10;
    dataChunkC->size = 10;
    dataChunkA->numSelectedValues = 10;
    dataChunkB->numSelectedValues = 10;
    dataChunkC->numSelectedValues = 10;
    dataChunkA->currPos = 0;
    dataChunkB->currPos = 2;
    dataChunkC->currPos = -1;
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ScanMockOpWithSelector::getNextTuples() {
    if (numTuples < 10) {
        dataChunks->getDataChunk(1)->currPos = numTuples;
        numTuples += 1;
        return;
    }
    dataChunks->getDataChunk(0)->size = 0;
    dataChunks->getDataChunk(1)->size = 0;
    dataChunks->getDataChunk(2)->size = 0;
    dataChunks->getDataChunk(0)->numSelectedValues = 0;
    dataChunks->getDataChunk(1)->numSelectedValues = 0;
    dataChunks->getDataChunk(2)->numSelectedValues = 0;
}

void ScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
    }
    dataChunkA->append(vectorA1);
    vectorA1->setDataChunkOwner(dataChunkA);
    dataChunkA->append(vectorA2);
    vectorA2->setDataChunkOwner(dataChunkA);
    dataChunkB->append(vectorB1);
    vectorB1->setDataChunkOwner(dataChunkB);
    dataChunkB->append(vectorB2);
    vectorB2->setDataChunkOwner(dataChunkB);
    dataChunkC->append(vectorC1);
    vectorC1->setDataChunkOwner(dataChunkC);
    dataChunkC->append(vectorC2);
    vectorC2->setDataChunkOwner(dataChunkC);

    dataChunkA->size = 10;
    dataChunkB->size = 10;
    dataChunkC->size = 10;
    dataChunkA->numSelectedValues = 10;
    dataChunkB->numSelectedValues = 10;
    dataChunkC->numSelectedValues = 10;
    for (uint64_t i = 0; i < 10; i++) {
        dataChunkB->selectedValuesPos[i] = 2;
    }
    dataChunkA->currPos = 0;
    dataChunkB->currPos = 0;
    dataChunkC->currPos = -1;
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}

void ProbeScanMockOpWithSelector::getNextTuples() {
    if (numTuples < 1) {
        dataChunks->getDataChunk(1)->currPos = numTuples;
        numTuples += 1;
        return;
    }
    dataChunks->getDataChunk(0)->size = 0;
    dataChunks->getDataChunk(1)->size = 0;
    dataChunks->getDataChunk(2)->size = 0;
    dataChunks->getDataChunk(0)->numSelectedValues = 0;
    dataChunks->getDataChunk(1)->numSelectedValues = 0;
    dataChunks->getDataChunk(2)->numSelectedValues = 0;
}

void ProbeScanMockOpWithSelector::generateDataChunks() {
    auto dataChunkA = make_shared<DataChunk>();
    auto dataChunkB = make_shared<DataChunk>();
    auto dataChunkC = make_shared<DataChunk>();

    NodeIDCompressionScheme compressionScheme;
    auto vectorA1 = make_shared<NodeIDVector>(18, compressionScheme);
    auto vectorA2 = make_shared<ValueVector>(INT32);
    auto vectorB1 = make_shared<NodeIDVector>(28, compressionScheme);
    auto vectorB2 = make_shared<ValueVector>(DOUBLE);
    auto vectorC1 = make_shared<NodeIDVector>(38, compressionScheme);
    auto vectorC2 = make_shared<ValueVector>(BOOL);

    for (int32_t i = 0; i < 10; i++) {
        vectorA1->setValue<uint64_t>(i, (uint64_t)i);
        vectorA2->setValue<int32_t>(i, (int32_t)(i * 2));
        vectorB1->setValue<uint64_t>(i, (uint64_t)i);
        vectorB2->setValue(i, (double)(i / 2));
        vectorC1->setValue<uint64_t>(i, (uint64_t)i);
        vectorC2->setValue(i, (bool)((i / 2) == 1));
    }
    dataChunkA->append(vectorA1);
    vectorA1->setDataChunkOwner(dataChunkA);
    dataChunkA->append(vectorA2);
    vectorA2->setDataChunkOwner(dataChunkA);
    dataChunkB->append(vectorB1);
    vectorB1->setDataChunkOwner(dataChunkB);
    dataChunkB->append(vectorB2);
    vectorB2->setDataChunkOwner(dataChunkB);
    dataChunkC->append(vectorC1);
    vectorC1->setDataChunkOwner(dataChunkC);
    dataChunkC->append(vectorC2);
    vectorC2->setDataChunkOwner(dataChunkC);

    dataChunkA->size = 10;
    dataChunkB->size = 10;
    dataChunkC->size = 10;
    dataChunkA->selectedValuesPos[0] = 0;
    dataChunkB->selectedValuesPos[0] = 2;
    dataChunkC->selectedValuesPos[0] = 3;
    dataChunkA->numSelectedValues = 1;
    dataChunkB->numSelectedValues = 1;
    dataChunkC->numSelectedValues = 1;
    dataChunkA->currPos = 0;
    dataChunkB->currPos = 0;
    dataChunkC->currPos = -1;
    dataChunks->append(dataChunkA);
    dataChunks->append(dataChunkB);
    dataChunks->append(dataChunkC);
}
