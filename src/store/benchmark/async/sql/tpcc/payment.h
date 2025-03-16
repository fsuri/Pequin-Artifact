/***********************************************************************
 *
 * Copyright 2021 Florian Suri-Payer <fsp@cs.cornell.edu>
 *                Matthew Burke <matthelb@cs.cornell.edu>
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
#ifndef SQL_PAYMENT_H
#define SQL_PAYMENT_H

#include "store/benchmark/async/sql/tpcc/tpcc_transaction.h"

namespace tpcc_sql {

class SQLPayment : public TPCCSQLTransaction {
 public:
  SQLPayment(uint32_t w_id, uint32_t c_c_last,
      uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen);
  virtual ~SQLPayment();

 protected:
  std::mt19937 &gen;

  uint32_t w_id;
  uint32_t d_id;
  uint32_t d_w_id;
  uint32_t c_w_id;
  uint32_t c_d_id;
  uint32_t c_id;
  uint32_t h_amount;
  uint32_t h_date;
  uint32_t random_row_id;
  bool c_by_last_name;
  std::string c_last;
};

class SQLPaymentSequential : public TPCCSQLTransaction {
 public:
  SQLPaymentSequential(uint32_t w_id, uint32_t c_c_last,
      uint32_t c_c_id, uint32_t num_warehouses, std::mt19937 &gen);
  SQLPaymentSequential(std::mt19937 &gen) : gen(gen) { };
  virtual ~SQLPaymentSequential();

 protected:
  std::mt19937 &gen;

  uint32_t w_id;
  uint32_t d_id;
  uint32_t d_w_id;
  uint32_t c_w_id;
  uint32_t c_d_id;
  uint32_t c_id;
  uint32_t h_amount;
  uint32_t h_date;
  uint32_t random_row_id;
  bool c_by_last_name;
  std::string c_last;
};

} // namespace tpcc_sql

#endif /* SQL_PAYMENT_H */
