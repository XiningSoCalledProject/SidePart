//
// DataPath.h — resolve test-data files without hard-coding an absolute path.
// Created 9/22/26.
//
// Several tests still contained paths like
//     /Users/xiningyuan/Desktop/seal-oram-netio-master-copy/Testing/Unit Tests/test_data.csv
// which broke the moment the repo was renamed (it is now .../DePart/DePart) and
// which can never work on another machine or on EC2.
//
// This header derives the repo root from its OWN location at compile time
// (__FILE__ is <repo>/include/DataPath.h), so it is correct regardless of the
// working directory the binary is launched from:
//
//     std::string csv = departDataPath("Testing/Unit Tests/test_data.csv");
//
// Override at run time with:   export DEPART_DATA_DIR=/path/to/repo
//
#ifndef DATA_PATH_H
#define DATA_PATH_H

#include <cstdlib>
#include <string>

inline std::string departRepoRoot() {
    if (const char* env = std::getenv("DEPART_DATA_DIR")) return std::string(env);
    std::string self = __FILE__;                     // <repo>/include/DataPath.h
    auto cut = [](const std::string& p) {
        const auto s = p.find_last_of("/\\");
        return s == std::string::npos ? std::string(".") : p.substr(0, s);
    };
    return cut(cut(self));                           // strip file, then include/
}

inline std::string departDataPath(const std::string& relative) {
    return departRepoRoot() + "/" + relative;
}

#endif  // DATA_PATH_H