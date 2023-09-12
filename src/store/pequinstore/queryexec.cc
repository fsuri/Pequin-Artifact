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


#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>


#include "store/pequinstore/server.h"

#include <bitset>
#include <queue>
#include <ctime>
#include <chrono>
#include <sys/time.h>
#include <sstream>
#include <list>
#include <utility>

#include "lib/assert.h"
#include "lib/tcptransport.h"
#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include "store/pequinstore/common.h"
#include "store/pequinstore/phase1validator.h"
#include "store/pequinstore/localbatchsigner.h"
#include "store/pequinstore/sharedbatchsigner.h"
#include "store/pequinstore/basicverifier.h"
#include "store/pequinstore/localbatchverifier.h"
#include "store/pequinstore/sharedbatchverifier.h"
#include "lib/batched_sigs.h"
#include <valgrind/memcheck.h>

namespace pequinstore {

//TODO: All Code for Query Execution Enginge goes here.

//e.g. SQL to Query plan
// AST execution
// Active Read Set Heuristics on AST + continuation execution.

} // namespace pequinstore