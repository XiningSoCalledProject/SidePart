#ifndef SEAL_ORAM_NETIO_CSV_READER_H
#define SEAL_ORAM_NETIO_CSV_READER_H

#include <boost/algorithm/string.hpp>
#include <iostream>
#include <map>
#include <fstream>
#include <sstream>
#include <string>
#include <vector>
#include <algorithm>
#include <bitset>
#include <string>
#include <random>

#ifndef PATH_MAX
#define PATH_MAX (4096)
#endif

std::string division(std::string str, int m, int n, int & remain);

std::string conversion(std::string str, int m, int n);

struct fieldType {
    std::string type_name;
    std::vector<uint32_t> type_bytes;
};

std::vector<uint8_t> stringToBytes(const std::string& inputString);

std::vector<std::string> split(const std::string &tuple_entry);

uint32_t fieldWidthBytes(const int & ordinal, const std::string & field_spec);

uint32_t tupleWidthBytesFromSchema(const std::string &schema_spec);

fieldType getFieldType(const std::string & field_spec);
std::vector<fieldType> tupleTypesFromSchema(const std::string &schema_spec);

std::vector<std::string> readTextFile(const std::string &filename);

std::string getFieldName(const std::string & field_spec);

std::vector<std::string> tupleNamesFromSchema(const std::string &schema_spec);

// map field name and type
std::map<std::string, fieldType> mapFieldNameTypeFromSchema(const std::string &schema_spec);

std::string getFieldValue(std::string tuple_entry, std::string schema, std::string field_name);

std::string getPrimaryKeyValue(std::string tuple_entry, std::string schema, std::vector<std::string> primary_keys);

std::vector<uint8_t> getFieldValueAsBytes(std::string tuple_entry, std::string schema, std::string field_name);

std::vector<uint8_t> getPrimaryKeyValueAsBytes(std::string tuple_entry, std::string schema, std::vector<std::string> primary_keys);

std::string formatTokenWithSchema(std::string token, fieldType field_type);

std::vector<std::string> readCSVFileWithSchema(const std::string &filename, const std::string &schema_spec);

int convertBytes2Num(std::string str, const std::string& field_type);

std::vector<std::vector<uint8_t>> convertStrVal2Bytes(std::vector<std::string> values, std::vector<fieldType> types);

std::vector<std::string> splitstring(const std::string& line, char delimiter = ',');

int findColumnIndex(const std::vector<std::string>& header, const std::string& key_name);

void sortSampledByPrimaryKey(std::vector<std::string>& sampled,
                             const std::string& schema_str,
                             const std::vector<std::string>& primary_keys);

std::vector<std::string> readCsvLines(const std::string& filename) ;

std::vector<std::string> samplePercentage(const std::vector<std::string>& data, double percentage);





#endif //SEAL_ORAM_NETIO_CSV_READER_H
