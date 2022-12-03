//
// Created by sakkammadam on 11/6/22.
//
#include "FileProcessorBase.hpp"
#include <map>
#include <vector>
#include <string>

class FileProcessorRedOutput: public FileProcessorBase{
public:
    // This constructor is used to support reducer operation
    FileProcessorRedOutput(
            const std::string &operation,
            const std::map<std::string, std::map<std::string, size_t>> &reduced_result
            ):FileProcessorBase(){
        // Sets the operation!
        this->setOperation(operation);
        // Sets the raw reducer output as the private data member!
        this->setRawReducerOutput(reduced_result);
    }

    // Override virtual method from the base class
    // This implementation will write the Reducer results to disk and save the final output directory as a private data member
    virtual void runOperation() {
        std::cout << "Operation: " << this->getOperation() << std::endl;
        if (this->getOperation() == "reducer"){
            std::cout << "Proceeding to write reducer data output to file system...." << std::endl;
            // Reducer data
            std::map<std::string, std::map<std::string, size_t>> reduceData = this->getRawReducerOutput();
            // Shuffle parent path
            std::string reducerParentPath;
            // Let's iterate over each map item in the vector
                // let's read corresponding map
            for (const auto &reduceFile: reduceData) {
                // this is the shuffle directory associated
                std::string reduceFileDir = reduceFile.first.substr(0, reduceFile.first.rfind('/') + 1);
                // capturing the grandparent path
                reducerParentPath = std::filesystem::path(reduceFileDir).parent_path().string();
                // We need to check if directory exists. if it doesn't - create the directory, else don't do anything
                // Creating shuffle directory if shuffle directory is not there already
                this->createDirectory(reduceFileDir);
                std::cout << "Proceeding to create " << reduceFile.first << std::endl;
                // declare an ostream
                std::ofstream outputFile;
                // create an empty file against the temp directory
                outputFile.open(reduceFile.first);
                // let's iterate over the map
                for (const auto &tokenDetails: reduceFile.second) {
                    std::string prepRow = "(" + tokenDetails.first + "," + std::to_string(tokenDetails.second) + ")";
                    outputFile << prepRow << std::endl;
                }
                // close the output file
                outputFile.close();
            }
            // save the grandparent path (final output path) as private data member for downstream use!
            this->setFinalOutputDirectory(reducerParentPath);
        }
    }
};

// Class factories - constructor
extern "C" FileProcessorBase* createInputObj(
        const std::string &operation,
        const std::map<std::string, std::map<std::string, size_t>> &reduced_raw){
    return new FileProcessorRedOutput(operation,reduced_raw);
}

// Class factories - destructor
extern "C" void removeInputObj(FileProcessorBase* fpObj){
    delete fpObj;
}