//
// Created by sakkammadam on 11/6/22.
//
#include "ReducerBase.hpp"
#include <iostream>
#include <fstream>
#include <string>
#include <filesystem>
#include <map>
#include <vector>

class Reducer:public ReducerBase{
public:
    // default constructor
    explicit Reducer(){
        // Nothing here!
    }
    // Explicit constructor call
    explicit Reducer(const std::string &shuffle_directory): ReducerBase(shuffle_directory){
        // All initialization happens through the abstract implementation (ReducerBase)
    }
    // Helper method!
    // This method will process a single shuffle file directory path and produce an object in memory
    // The memory object is a map of the directory (fileName) and its value is another map
    // that contains all tokens across all partitions and their total counts
    std::map<std::string, std::map<std::string, size_t>> reduceFile(const std::string &fullyQualifiedShuffleFilePath){
        // Declare the reduced file object
        std::map<std::string, std::map<std::string, size_t>> singleReducedFile;
        // Check if the file directory is present in the file system
        std::filesystem::path directoryPath(fullyQualifiedShuffleFilePath);
        // Do the following only if the directory exists in the file system!
        if (std::filesystem::is_directory(directoryPath)) {
            std::cout << "Reducing directory path - " << fullyQualifiedShuffleFilePath << std::endl;
            // Create temp objects
            std::map<std::string, size_t> tempReduce;
            for (const auto &entry: std::filesystem::directory_iterator(fullyQualifiedShuffleFilePath)) {
                // Checking if the underlying path is a file
                if (std::filesystem::is_regular_file(entry.path())) {
                    // if it's a file - push it into vector
                    // Read the file as an input stream
                    std::ifstream inputFile(entry.path().string());
                    // Declare a string for each line in the file
                    std::string fileLine;
                    // parse the file line by line
                    while (std::getline(inputFile, fileLine)) {
                        // Capturing the token using substr - it will extract from 2nd position upto the comma occurrence
                        std::string token = fileLine.substr(1, fileLine.find(',') - 1);
                        // Capturing the occurrence using substr - it will extract from the comma to closing parentheses
                        // We use stoi to convert to integer
                        size_t tokenOccurrence = std::stoi(fileLine.substr(
                                fileLine.find(',') + 1, (fileLine.find(')') - fileLine.find(',')) - 1));
                        // Now will check if the token exists in tempReduce
                        // Let's create an iterator that will check tempShuffle for the token
                        auto mapItr = tempReduce.find(token);
                        // Check if iterator was exhausted
                        if (mapItr == tempReduce.end()) {
                            // if parsedToken was not found! - lets insert into tempShuffle
                            tempReduce.insert({token, tokenOccurrence});
                        } else {
                            // token was found! - let's increment the existing value with the token's current occurrence (within a partition file)
                            mapItr->second = (mapItr->second) + tokenOccurrence;
                        }
                    }
                }
            }
            // at this point, tempShuffle has the complete token counts of a partition file
            // lets create a new fully qualified path where we will replace "/temp_shuffler/" with "/final_output/"
            std::string fullyQualifiedOutFilePath = fullyQualifiedShuffleFilePath;
            // the string we are trying to replace
            std::string toReplace("/temp_shuffler/");
            // find its position within the file path
            size_t pos = fullyQualifiedOutFilePath.find(toReplace);
            // lets replace
            fullyQualifiedOutFilePath.replace(pos, toReplace.length(), "/final_output/");
            // we will simply attach it to the desired output file and save the result to singleReducedFile object
            singleReducedFile.insert({fullyQualifiedOutFilePath, tempReduce});
        } else {
            std::cout << "** Warning! ** Shuffle directory: " << fullyQualifiedShuffleFilePath << "doesn't exist in the file system!" << std::endl;
        }
        // final return
        return singleReducedFile;
    }

    // Virtual method to run reduce operations
    // Primary method that will act on shuffled files and create reduced results in memory
    // This method will operate against each processed partition file within the shuffle directory
    virtual void runReduceOperations(){
        std::map<std::string, std::map<std::string, size_t>> reducedDir = this->reduceFile(this->getShuffleOutputDirectory());
        // save object as private data member for downstream processing
        this->setReducedOutput(reducedDir);
    }
};

// Class factories - constructor
extern "C" ReducerBase* createInputObj(const std::string &shuffle_directory){
    return new Reducer(shuffle_directory);
}

// Class factories - destructor
extern "C" void removeInputObj(ReducerBase* rObj){
    delete rObj;
}