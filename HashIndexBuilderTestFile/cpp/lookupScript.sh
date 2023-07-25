#!/bin/bash

# Loop over numThreads from 1 to 8
for ((numThreads=1; numThreads<=8; numThreads++))
do
    echo "Testing with numThreads = $numThreads"

    # Run the program with the specified number of threads
    ./lookupThread-cpp $numThreads

    echo "---------------------------------------"
done

