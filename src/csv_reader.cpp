#include "csv_reader.h"
#include <sstream>
#include <unordered_map>

std::string division(std::string str, int m, int n, int & remain){
    std::string result = "";
    int a;
    remain = 0;

    for(int i = 0; i < str.size(); i++){
        a = (n * remain + (str[i] - '0'));
        str[i] = a / m + '0';
        remain = a % m;
    }
    //去掉多余的0 比如10/2=05
    int pos = 0;
    while(str[pos] == '0'){
        pos++;
    }
    return str.substr(pos);
}

std::string conversion(std::string str, int m, int n){
    std::string result = "";
    char c;
    int a;
    while(str.size() != 0){
        str = division(str, m , n, a);
        result = char(a + '0') +result;

    }
    return result;
}

std::vector<uint8_t> stringToBytes(const std::string& inputString) {
    std::vector<uint8_t> result;

    for (char character : inputString) {
        // Convert each character to its ASCII representation
        result.push_back(static_cast<uint8_t>(character));
    }
    return result;
}

std::vector<std::string> split(const std::string &tuple_entry) {
    std::vector<std::string> tokens, result;
    std::string latest_entry;
    boost::split(tokens, tuple_entry, boost::is_any_of(", "));

    // make a pass and merge any strings
    for(size_t i = 0; i < tokens.size(); ++i) {
        std::string token = tokens[i];
        if(token[0] == '"' && latest_entry.empty()) {  // starts with " and the beginning of an entry
            latest_entry = token;
        }
        else if(latest_entry.empty()) {
            result.push_back(token);
        }
        else  {
            latest_entry += "," + token;
            if(*(--token.end()) == '"') { // if it is the end of the string
                result.push_back(latest_entry.substr(1, latest_entry.length() - 2)); // chop off leading and trailing double-quotes
                latest_entry.clear();
            }
        }
    } // end for
    return result;
}

uint32_t fieldWidthBytes(const int & ordinal, const std::string & field_spec) {

    std::string table_name_;
    std::string field_name_;
    uint32_t field_width_ = 0;
    uint32_t string_length_ = 0;


    auto dot_pos = field_spec.find('.');
    auto colon_pos = field_spec.find(':');
    if (dot_pos != std::string::npos) {
        assert(dot_pos < colon_pos);
        table_name_ = field_spec.substr(0, dot_pos);
        field_name_ = field_spec.substr(dot_pos + 1, colon_pos - dot_pos - 1);
    } else {
        field_name_ = field_spec.substr(0, colon_pos);
    }

    std::string type_str = field_spec.substr(colon_pos + 1);
    if (type_str.find('(') != std::string::npos) {
        auto open_paren_pos = type_str.find('(');
        auto close_paren_pos = type_str.find(')');
        std::string length_str = type_str.substr(open_paren_pos + 1, close_paren_pos - open_paren_pos - 1);
        type_str = type_str.substr(0, open_paren_pos);
        if (type_str == "numeric") {
            auto dash_pos = length_str.find('-');
            field_width_ = std::stoi(length_str.substr(0, dash_pos));
            if (field_width_ < 9) {
                field_width_ = 5;
            }
            else if (field_width_ < 19) {
                field_width_ = 9;
            }
            else if (field_width_ < 28) {
                field_width_ = 13;
            }
            else {
                field_width_ = 17;
            }
        }
        else {
            field_width_ = std::stoi(length_str);
        }
    }
    else {
        if (type_str == "int8") {
            field_width_ = 1;
        }
        if (type_str == "int16") {
            field_width_ = 2;
        }
        if (type_str == "int32") {
            field_width_ = 4;
        }
        if (type_str == "float") {
            field_width_ = 4;
        }
        if (type_str == "double") {
            field_width_ = 8;
        }
        if (type_str == "date") {
            field_width_ = 10;
        }
    }

    return field_width_;
}

uint32_t tupleWidthBytesFromSchema(const std::string &schema_spec) {
    std::string schema_str = schema_spec;
    // chop off any trailing whitespace or endlines
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // chop off parens
    if(schema_str.at(0) == '(' && schema_str.at(schema_str.length() - 1) == ')') {
        schema_str = schema_str.substr(1, schema_str.length() - 2);
    }
    // delete any spaces
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // split on commas
    std::vector<std::string> tokens = split(schema_str);
    int counter = 0;
    int last_field = tokens.size() - 1;
    uint32_t tuple_width_bytes = 0;
    for(auto token : tokens) {
        tuple_width_bytes += fieldWidthBytes(counter, token);
        //std::cout << "Print now tuple width bytes " << tuple_width_bytes << std::endl;
        if(counter == last_field) {
            // do not add dummy tag
            break;
        }
        ++counter;
    }
    return tuple_width_bytes;
}

fieldType getFieldType(const std::string & field_spec) {
    std::string table_name_;
    std::string field_name_;
    uint32_t field_width_ = 0;
    uint32_t string_length_ = 0;
    fieldType field_type_;

    auto dot_pos = field_spec.find('.');
    auto colon_pos = field_spec.find(':');
    if (dot_pos != std::string::npos) {
        assert(dot_pos < colon_pos);
        table_name_ = field_spec.substr(0, dot_pos);
        field_name_ = field_spec.substr(dot_pos + 1, colon_pos - dot_pos - 1);
    } else {
        field_name_ = field_spec.substr(0, colon_pos);
    }

    std::string type_str = field_spec.substr(colon_pos + 1);
    if (type_str.find('(') != std::string::npos) {
        auto open_paren_pos = type_str.find('(');
        auto close_paren_pos = type_str.find(')');
        std::string length_str = type_str.substr(open_paren_pos + 1, close_paren_pos - open_paren_pos - 1);
        type_str = type_str.substr(0, open_paren_pos);
        if (type_str == "numeric") {
            field_type_.type_name = "numeric";
            auto dash_pos = length_str.find('-');
            field_width_ = std::stoi(length_str.substr(0, dash_pos));
            if (field_width_ < 9) {
                field_width_ = 5;
            }
            else if (field_width_ < 19) {
                field_width_ = 9;
            }
            else if (field_width_ < 28) {
                field_width_ = 13;
            }
            else {
                field_width_ = 17;
            }
            field_type_.type_bytes.push_back(field_width_);
            field_type_.type_bytes.push_back(std::stoi(length_str.substr(dash_pos+1)));
        }
        else {
            field_type_.type_name = "char";
            field_width_ = std::stoi(length_str);
            field_type_.type_bytes.push_back(field_width_);
        }
    }
    else {
        if (type_str == "int8") {
            field_type_.type_name = "int8";
            field_width_ = 1;
        }
        if (type_str == "int16") {
            field_type_.type_name = "int16";
            field_width_ = 2;
        }
        if (type_str == "int32") {
            field_type_.type_name = "int32";
            field_width_ = 4;
        }
        if (type_str == "float") {
            field_type_.type_name = "float";
            field_width_ = 4;
        }
        if (type_str == "double") {
            field_type_.type_name = "double";
            field_width_ = 8;
        }
        if (type_str == "date") {
            field_type_.type_name = "date";
            field_width_ = 10;
        }
        field_type_.type_bytes.push_back(field_width_);
    }

    return field_type_;
}

std::vector<fieldType> tupleTypesFromSchema(const std::string &schema_spec) {
    std::vector<fieldType> field_types;
    std::string schema_str = schema_spec;
    // chop off any trailing whitespace or endlines
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // chop off parens
    if(schema_str.at(0) == '(' && schema_str.at(schema_str.length() - 1) == ')') {
        schema_str = schema_str.substr(1, schema_str.length() - 2);
    }
    // delete any spaces
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // split on commas
    std::vector<std::string> tokens = split(schema_str);
    int counter = 0;
    int last_field = tokens.size() - 1;
    uint32_t tuple_width_bytes = 0;
    for(auto token : tokens) {
        field_types.push_back(getFieldType(token));
        if(counter == last_field) {
            // do not add dummy tag
            break;
        }
        ++counter;
    }
    return field_types;
}

std::vector<std::string> readTextFile(const std::string &filename) {
    std::vector<std::string> lines;
    std::ifstream input(filename);
    std::string line;

    if(!input) {
        throw std::invalid_argument("Unable to open file: " + filename);
    }

    while (std::getline(input, line)) {
        lines.push_back(line);
    }

    return lines;
}

std::string getFieldName(const std::string & field_spec) {
    std::string table_name_;
    std::string field_name_;
    uint32_t field_width_ = 0;
    uint32_t string_length_ = 0;

    auto dot_pos = field_spec.find('.');
    auto colon_pos = field_spec.find(':');
    if (dot_pos != std::string::npos) {
        assert(dot_pos < colon_pos);
        table_name_ = field_spec.substr(0, dot_pos);
        field_name_ = field_spec.substr(dot_pos + 1, colon_pos - dot_pos - 1);
    } else {
        field_name_ = field_spec.substr(0, colon_pos);
    }
    return field_name_;
};

std::vector<std::string> tupleNamesFromSchema(const std::string &schema_spec) {
    std::string schema_str = schema_spec;
    // chop off any trailing whitespace or endlines
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // chop off parens
    if(schema_str.at(0) == '(' && schema_str.at(schema_str.length() - 1) == ')') {
        schema_str = schema_str.substr(1, schema_str.length() - 2);
    }
    // delete any spaces
    schema_str.erase(std::remove_if(schema_str.begin(), schema_str.end(), ::isspace), schema_str.end());
    // split on commas
    std::vector<std::string> tokens = split(schema_str);
    int counter = 0;
    int last_field = tokens.size() - 1;
    std::vector<std::string> tuple_names;

    for(auto token : tokens) {
        tuple_names.push_back(getFieldName(token));
        if(counter == last_field) {
            break;
        }
        ++counter;
    }
    return tuple_names;
}

// map field name and type
std::map<std::string, fieldType> mapFieldNameTypeFromSchema(const std::string &schema_spec) {
    std::map<std::string, fieldType> map_name_type;
    std::vector<std::string> field_names = tupleNamesFromSchema(schema_spec);
    std::vector<fieldType> field_types = tupleTypesFromSchema(schema_spec);
    // assert ?
    size_t size = std::min(field_names.size(), field_types.size());

    for (size_t i = 0; i < size; ++i) {
        map_name_type[field_names[i]] = field_types[i];
    }
    return map_name_type;
}

std::string getFieldValue(std::string tuple_entry, std::string schema, std::string field_name) {
    uint32_t start_bytes = 0;
    std::vector<fieldType> field_type = tupleTypesFromSchema(schema);
    std::vector<std::string> field_str = tupleNamesFromSchema(schema);

    uint32_t idx = 0;
    for (const auto& element : field_type) {
        if (field_str[idx] == field_name) {
            std::string sub_tuple = tuple_entry.substr(start_bytes, element.type_bytes[0]);
            return sub_tuple;
        } else {
            start_bytes += element.type_bytes[0];
        }
        idx += 1;
    }
    return "";
}

std::string getPrimaryKeyValue(std::string tuple_entry, std::string schema, std::vector<std::string> primary_keys) {
    std::string primary_keys_value;
    for (const auto& element : primary_keys) {
        std::string sub_primary_keys_value = getFieldValue(tuple_entry, schema, element);
        primary_keys_value += sub_primary_keys_value;
    }
    return primary_keys_value;
}

std::vector<uint8_t> getFieldValueAsBytes(std::string tuple_entry, std::string schema, std::string field_name) {
    return stringToBytes(getFieldValue(tuple_entry, schema, field_name));
}

std::vector<uint8_t> getPrimaryKeyValueAsBytes(std::string tuple_entry, std::string schema, std::vector<std::string> primary_keys) {
    return stringToBytes(getPrimaryKeyValue(tuple_entry, schema, primary_keys));
}

std::string formatTokenWithSchema(std::string token, fieldType field_type) {
    std::string new_line;
    int expectedSize = field_type.type_bytes[0];
    if (token == "null") {
        for(int j = 0; j < field_type.type_bytes[0]; ++j) {
            new_line.push_back('\0');
        }
        return new_line;
    }
    if (field_type.type_name == "int32") {
        //std::cout << "Add 4 bytes!" << std::endl;
        if (token.size() != expectedSize) {}
        uint32_t tmp = std::stoi(token);
        char tmp_str[4];
        memcpy(tmp_str, &tmp, 4);
        // new_line.append(tmp_str);
        new_line.append(tmp_str, 4);
    }
    else if (field_type.type_name == "int16") {
        //std::cout << "Add 2 bytes!" << std::endl;
        if (token.size() != expectedSize) {}
        uint32_t tmp = std::stoi(token);
        char tmp_str[2];
        memcpy(tmp_str, &tmp, 2);
        //std::cout << "Appending " << (int) tmp_str[0] << " " << (int) tmp_str[1] << " to new line" << std::endl;
        // new_line.append(tmp_str);
        new_line.append(tmp_str, 2);
    }
    else if (field_type.type_name == "int8") {
        //std::cout << "Add 1 byte!" << std::endl;
        if (token.size() != expectedSize) {}
        uint32_t tmp = std::stoi(token);
        char tmp_str[1];
        memcpy(tmp_str, &tmp, 1);
        // new_line.append(tmp_str);
        new_line.append(tmp_str, 1);
    }
    else if (field_type.type_name == "char") {
        if (token.size() != expectedSize) {}
        token.resize(field_type.type_bytes[0]);
        //std::cout << "Add " << field_type[i].type_bytes[0] << " bytes!" << std::endl;
        new_line.append(token);
    }
    else if (field_type.type_name == "numeric") {
        if (token.size() != expectedSize) {}
        token.erase(std::remove(token.begin(), token.end(), '.'), token.end());
        // sign
        auto found = token.find('-');
        char sign;
        if (found != std::string::npos) {
            sign = '\xff';
            token = token.substr(1);
        } else {
            sign = '\x00';
        }
        new_line.push_back(sign);
        // convert to int
        std::string binary = conversion(token, 2, 10);
        if (field_type.type_bytes[0] == 5) {
            //std::cout << "Come to the numeric with bytes 5!" << std::endl;
            std::bitset<32> b3(binary);
            char b_to_string[4];
            memcpy(b_to_string, &b3, 4);
            new_line.append(b_to_string, 4);
        } else if (field_type.type_bytes[0] == 9) {
            //std::cout << "Come to the numeric with bytes 9!" << std::endl;
            std::bitset<64> b3(binary);
            char b_to_string[8];
            memcpy(b_to_string, &b3, 8);
            new_line.append(b_to_string, 8);
        } else if (field_type.type_bytes[0] == 13) {
            //std::cout << "Come to the numeric with bytes 13!" << std::endl;
            std::bitset<96> b3(binary);
            char b_to_string[12];
            memcpy(b_to_string, &b3, 12);
            new_line.append(b_to_string, 12);
        } else {
            //std::cout << "Come to the numeric with bytes 17!" << std::endl;
            std::bitset<128> b3(binary);
            char b_to_string[16];
            memcpy(b_to_string, &b3, 16);
            new_line.append(b_to_string, 16);
        }
    } else if (field_type.type_name == "date"){
        token.resize(field_type.type_bytes[0]);
        new_line.append(token);
    }
    else {
        new_line.append(token);
    }
    return new_line;
}

std::vector<std::string> readCSVFileWithSchema(const std::string &filename, const std::string &schema_spec) {
    std::cout << "I can run here successfully." << std::endl;
    std::vector<std::string> lines;
    std::ifstream input(filename);
    std::string line;

    std::vector<fieldType> field_type = tupleTypesFromSchema(schema_spec);

    std::cout << "The size of the field type is " << field_type.size() << std::endl;

    if(!input) {
        throw std::invalid_argument("Unable to open file: " + filename);
    }

    int counter = 0;
    while (std::getline(input, line)) {
        // read line
        // parse line to fields, process fields, and convert back to bytes string
        std::string new_line;
        std::vector<std::string> tokens = split(line);
        if (line == "") {
            //new_line = "";
            continue;
        }
        for (int i = 0; i < field_type.size(); ++i) {
            std::string token = tokens[i];
            int expectedSize = field_type[i].type_bytes[0];

            new_line.append(formatTokenWithSchema(token, field_type[i]));
        }
        lines.push_back(new_line);
        counter++;
    }
    return lines;
}


int convertBytes2Num(std::string str, const std::string& field_type) {
    std::vector<char> bytes(str.begin(), str.end());
    if (field_type == "int8") {
        uint8_t result = static_cast<unsigned char>(bytes[0]);
        return static_cast<int>(result);
    } else if (field_type == "int16") {
        uint16_t result = 0;
        for (int j = 1; j >= 0; --j) {
            result = (result << 8) | static_cast<unsigned char>(bytes[j]);
        }
        return static_cast<int>(result);
    } else if (field_type == "int32") {
        uint32_t result = 0;
        for (int j = 3; j >= 0; --j) {
            result = (result << 8) | static_cast<unsigned char>(bytes[j]);
        }
        return static_cast<int>(result);
    }
    return 0;
}


std::vector<std::vector<uint8_t>> convertStrVal2Bytes(std::vector<std::string> values, std::vector<fieldType> types) {
    std::vector<std::vector<uint8_t >> new_val;
    int idx = 0;
    while (idx < values.size()) {
        std::string val = values[idx];
        fieldType type = types[idx];
        std::string format_val = formatTokenWithSchema(val, type);
        new_val.push_back(stringToBytes(format_val));
        idx++;
    }
    return new_val;
}

// read csv file and find the primary key;
std::vector<std::string> splitstring(const std::string& line, char delimiter) {
    std::vector<std::string> tokens;
    std::string token;
    std::istringstream ss(line);
    while (getline(ss, token, delimiter)) {
        tokens.push_back(token);
    }
    return tokens;
}

int findColumnIndex(const std::vector<std::string>& header, const std::string& key_name) {
    for (size_t i = 0; i < header.size(); ++i) {
        if (header[i] == key_name) return i;
    }
    throw std::invalid_argument("Primary key not found in header");
}

// sort data inside the table;
void sortSampledByPrimaryKey(std::vector<std::string>& sampled,
                             const std::string& schema_str,
                             const std::vector<std::string>& primary_keys) {
    // 1. Parse field names from schema
    std::vector<std::string> field_names = tupleNamesFromSchema(schema_str);
    std::unordered_map<std::string, int> field_name_to_index;
    for (size_t i = 0; i < field_names.size(); ++i) {
        field_name_to_index[field_names[i]] = i;
    }

    // 2. Extract sort key for each line
    std::vector<std::pair<std::string, std::string>> key_line_pairs;
    for (const std::string& line : sampled) {
        std::vector<std::string> tokens = splitstring(line, ',');
        std::string key;
        for (const std::string& pk : primary_keys) {
            int idx = field_name_to_index.at(pk);
            key += tokens[idx] + "#";  // join PKs
        }
        key_line_pairs.emplace_back(key, line);
    }

    // 3. Sort by key
    std::sort(key_line_pairs.begin(), key_line_pairs.end(),
              [](const auto& a, const auto& b) { return a.first < b.first; });

    // 4. Write back to sampled
    sampled.clear();
    for (const auto& pair : key_line_pairs) {
        sampled.push_back(pair.second);
    }
}


// choose xx% of data from the csv file as the original data;
std::vector<std::string> readCsvLines(const std::string& filename) {
    std::ifstream file(filename);
    std::vector<std::string> lines;
    std::string line;

    while (getline(file, line)) {
        lines.push_back(line);
    }
    return lines;
}

std::vector<std::string> samplePercentage(const std::vector<std::string>& data, double percentage) {
    size_t total = data.size();
    size_t sample_size = static_cast<size_t>(percentage * total);

    std::vector<std::string> shuffled = data;  // copy
    std::random_device rd;
    std::mt19937 gen(rd());
    std::shuffle(shuffled.begin(), shuffled.end(), gen);  // shuffle

    return std::vector<std::string>(shuffled.begin(), shuffled.begin() + sample_size);
}
