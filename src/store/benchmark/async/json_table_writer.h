#include<iostream>
#include <stdio.h>
#include <sys/time.h>
#include <time.h>
#include <stdlib.h>

#include <cstring>
#include <vector>
#include <iomanip>

#include <fstream>
#include <nlohmann/json.hpp>

using json = nlohmann::json;

Class TableWriter {
    public:
        TableWriter();
        virtual ~TableWriter();
        void add_table(std::string &table_name, std::vector<std::string&>& column_names, std::vector<std::string&>& column_types, const std::vector<uint32_t> primary_key_col_idx )
        void add_row(std::string &table_name, const std::vector<std::string> &values)
        void flush(std::string &file_name)
    private:
        json tables;
};

