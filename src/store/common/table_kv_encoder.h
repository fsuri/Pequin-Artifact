/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *              
 * Permission is hereby granted, free of charge, to any person
 * obtaining a copy of this software and associated documentation
 * files (the "Software"), to deal in the Software without
 * restriction, including without limitation the rights to use, copy,
 * modify, merge, publish, distribute, sublicense, and/or sell copies
 * of the Software, and to permit persons to whom the Software is
 * furnished to do so, subject to the following conditions:
 * 
 * The above copyright notice and this permission notice shall be
 * included in all copies or substantial portions of the Software.
 * 
 * THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND,
 * EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF
 * MERCHANTABILITY, FITNESS FOR A PARTICULAR PURPOSE AND
 * NONINFRINGEMENT. IN NO EVENT SHALL THE AUTHORS OR COPYRIGHT HOLDERS
 * BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER LIABILITY, WHETHER IN AN
 * ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM, OUT OF OR IN
 * CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
 * SOFTWARE.
 *
 **********************************************************************/
#ifndef _ENCODER_H_
#define _ENCODER_H_

#include <vector>
#include <string>
#include <map>
#include <string_view>

static std::string unique_delimiter = "#";

static std::map<std::string, uint64_t> name_to_numerics;
static std::map<std::string, std::string> numerics_to_name;
//static std::vector<std::string> numeric_to_name;

std::string EncodeTable(const std::string &table_name);
std::string* DecodeTable(const std::string &numeric);

//For managing CC-store WriteSet
std::string EncodeTableRow(const std::string &table_name, const std::vector<std::string> &primary_key_column_values);
std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string*> &primary_key_column_values);
std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string*> &primary_key_column_values, const std::vector<bool>* p_col_quotes);
std::string EncodeTableRow(const std::string &table_name, const std::vector<const std::string_view*> &primary_key_column_values);
std::string EncodeTableCol(const std::string &table_name, const std::string &col_name);
void DecodeTableRow(const std::string &enc_key, std::string &table_name, std::vector<std::string> &primary_key_column_values);


#endif 