//
// Created by sakkammadam on 11/6/22.
//
#include "FileProcessorBase.hpp"
#include <map>
#include <vector>
#include <string>

class FileProcessorShufOutput:public FileProcessorBase{
public:
    // This constructor is used to support shuffler operation
    FileProcessorShufOutput(
            const std::string &operation,
            const std::vector<std::map<std::string, std::map<std::string,size_t>>> &shuffle_raw
            ):FileProcessorBase(){
        // Sets the operation!
        this->setOperation(operation);
        // Sets the raw shuffler output as the private data member!
        this->setRawShufflerOutput(shuffle_raw);
    }

    // Override virtual method from the base class
    // This implementation will write the shuffler results to disk and save the shuffler directory as a private data member
    virtual void runOperation() {
        std::cout << "Operation: " << this->getOperation() << std::endl;
        if (this->getOperation() == "shuffler"){
            std::cout << "Proceeding to write Shuffler data output to file system...." << std::endl;
            // Shuffle data
            std::vector<std::map<std::string, std::map<std::string, size_t>>> shuffleData = this->getRawShufflerOutput();
            // Shuffle parent path
            std::string shuffleParentPath;
            // Let's iterate over each map item in the vector
            for(const auto& shufflePartition:shuffleData){
                // let's read corresponding map
                for(const auto &shufflePartitionDetails:shufflePartition){
                    // this is the shuffle directory associated
                    std::string shuffleDirectory = shufflePartitionDetails.first.substr(0, shufflePartitionDetails.first.rfind('/') + 1);
                    // capturing the parent path
                    shuffleParentPath = std::filesystem::path(shuffleDirectory).parent_path().parent_path().string();
                    // We need to check if directory exists. if it doesn't - create the directory, else don't do anything
                    // Creating shuffle directory if shuffle directory is not there already
                    this->createDirectory(shuffleDirectory);
                    std::cout << "Proceeding to create " << shufflePartitionDetails.first << std::endl;
                    // declare an ostream
                    std::ofstream outputFile;
                    // create an empty file against the temp directory
                    outputFile.open(shufflePartitionDetails.first);
                    // let's iterate over the map
                    for(const auto &tokenDetails:shufflePartitionDetails.second){
                        std::string prepRow = "(" + tokenDetails.first + "," + std::to_string(tokenDetails.second) + ")";
                        outputFile << prepRow << std::endl;
                    }
                    // close the output file
                    outputFile.close();
                }
            }
            // save shuffleParentPath as private data member for downstream use!
            this->setShufflerOutputDirectory(shuffleParentPath);
        }
    }
};

// Class factories - constructor
extern "C" FileProcessorBase* createInputObj(
        const std::string &operation,
        const std::vector<std::map<std::string, std::map<std::string,size_t>>> &shuffle_raw){
    return new FileProcessorShufOutput(operation,shuffle_raw);
}

// Class factories - destructor
extern "C" void removeInputObj(FileProcessorBase* fpObj){
    delete fpObj;
}