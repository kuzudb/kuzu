#include <iostream>
#include <fstream>
#include <random>
#include <vector>

using namespace std;

int main() {

    int sizes[] = {5000, 10000, 20000, 50000, 100000, 150000, 200000, 300000, 500000, 700000, 
                    1000000, 1500000, 2000000, 3000000, 4000000, 5000000, 6000000, 
                    7000000, 8000000, 9000000, 10000000};

    for (int size: sizes) {
        ofstream outFile("csv/vPerson" + std::to_string(size) + ".csv");
        for (int i = 0; i < size; i++) {
            outFile << std::to_string(i+1) + "\n";
        }
        outFile.close();
    }
/*
    for (int size: sizes) {
        std::vector<int> arr;

        for (int i = 0; i < size; ++i) {
            arr.emplace_back(i+1);
        }

        std::random_device rd;
        std::mt19937 rng(rd());
        std::shuffle(arr.begin(), arr.end(), rng);

        ofstream outFile("csv/vPersonRandom" + std::to_string(size) + ".csv");
        for (int i = 0; i < size; i++) {
            outFile << std::to_string(arr[i]) + "\n";
        }
        outFile.close();
    }
    */
}