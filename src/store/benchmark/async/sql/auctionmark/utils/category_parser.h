/***********************************************************************
 *
 * Copyright 2023 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Liam Arzola <lma77@cornell.edu>
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
#ifndef AUCTIONMARK_CATEGORY_PARSER_H
#define AUCTIONMARK_CATEGORY_PARSER_H

#include <stdint.h>
#include <string>
#include <unordered_map>
#include <optional>

namespace auctionmark {

class Category {
 public:
  Category(uint32_t c_id, std::optional<uint32_t> c_parent_id, uint32_t item_cnt,
      const std::string &name, bool is_leaf);
  virtual ~Category();

  Category() = default;

  uint32_t get_category_id() const;
  std::optional<uint32_t> get_parent_category_id() const;
  uint32_t get_item_cnt() const;
  std::string get_name() const;
  bool is_leaf() const;

  bool operator ==(const Category &other) const;
  
 private:
  uint32_t c_id;
  std::optional<uint32_t> c_parent_id;
  uint32_t item_cnt;
  std::string name;
  bool leaf;
};

class CategoryHash {
 public:
  size_t operator()(const Category& c) const;
};

class CategoryParser {
 public:
  CategoryParser();
  virtual ~CategoryParser();

  std::unordered_map<std::string, Category> get_categories() const;
  void normalize_name(std::string &s);

 private:
  auto add_new_category(std::string full_category_name, int item_cnt, bool is_leaf) -> Category;
  void extract_category(std::string s);
  std::unordered_map<std::string, Category> categories;
  int32_t next_category_id;
};

}

#endif
