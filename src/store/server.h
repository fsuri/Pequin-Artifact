/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#ifndef STORE_SERVER_H
#define STORE_SERVER_H

#include "store/common/timestamp.h"
#include "store/common/stats.h"
#include "lib/message.h"
#include <string>

typedef std::vector<std::string> row_t;
typedef std::vector<row_t> row_segment_t;

class Server {
 public:
  Server() { }
  virtual ~Server(){}
  virtual void Load(const std::string &key, const std::string &value,
      const Timestamp timestamp) = 0;

  inline virtual void CreateTable(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx){Panic("This store does not support SQL Table operations");}

  inline virtual void CreateIndex(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::string &index_name, const std::vector<uint32_t> &index_col_idx){Panic("This store does not support Creating Indexes");}

  inline virtual void CacheCatalog(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<uint32_t> &primary_key_col_idx){Notice("This store does not need to cache catalog (Pequin only)");}

  inline virtual void LoadTableData(const std::string &table_name, const std::string &table_data_path, 
      const std::vector<std::pair<std::string, std::string>> &column_names_and_types, const std::vector<uint32_t> &primary_key_col_idx){Panic("This store does not support Loading Data from CSV");}

  inline virtual void LoadTableRows(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<row_t> *values, const std::vector<uint32_t> &primary_key_col_idx, int segment_no = 1, bool load_cc = true){Panic("This store does not support loading Data from Rows");}
    
  //[[deprecated]]
  inline virtual void LoadTableRow(const std::string &table_name, const std::vector<std::pair<std::string, std::string>> &column_data_types, 
      const std::vector<std::string> &row_values, const std::vector<uint32_t> &primary_key_col_idx ){Panic("This store does not support SQL Table operations");}

  virtual Stats &GetStats() = 0;
};

#endif /* STORE_SERVER_H */
