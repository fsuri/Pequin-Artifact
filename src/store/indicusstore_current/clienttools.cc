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
#include "store/indicusstore/common.h"

#include <sstream>
#include <list>

#include <cryptopp/sha.h>
#include <cryptopp/blake2.h>

#include "store/common/timestamp.h"
#include "store/common/transaction.h"
#include <utility>

#include "store/indicusstore/clienttools.h"

#include "lib/batched_sigs.h"

namespace indicusstore {


//should this simply include the client.h files too? (and remove this .h?)

//TODO: When signing p1 messages, forwarding them should have the signed message too
void SendMessage(this, group, *transport, int replica, message, bool sign = false, bool multithread = false){

  Callback (transport->SendMessageToReplica(this, group, rindex, phase2);)

  if(sign)
      if(multithread)
  
  else{
    SendCB();
  }
  return;
}

void SendMessageToGroup(){
  transport->SendMessageToGroup(this, group, phase2);
}


SendMessageToREplica

} // namespace indicusstore
