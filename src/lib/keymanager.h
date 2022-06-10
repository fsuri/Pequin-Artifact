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
#ifndef LIB_KEYMANAGER_H
#define LIB_KEYMANAGER_H

#include "lib/crypto.h"
#include "lib/message.h"

#include <map>
#include <mutex>

class KeyManager {
 public:
  KeyManager(const std::string &keyPath, crypto::KeyType t, bool precompute, uint64_t num_replicas = 0, uint64_t num_clients = 0);
  virtual ~KeyManager();

  crypto::PubKey* GetPublicKey(uint64_t id);
  crypto::PrivKey* GetPrivateKey(uint64_t id);
  void PreLoadPubKeys(bool isServer);
  void PreLoadPrivKey(uint64_t id, bool isClient);
  uint64_t GetClientKeyId(uint64_t client_id);


 private:
  const std::string keyPath;
  const crypto::KeyType keyType;
  const bool precompute;
  std::map<uint64_t, crypto::PubKey*> publicKeys;
  std::map<uint64_t, crypto::PrivKey*> privateKeys;
  std::mutex keyMutex;
  uint64_t num_replicas; //defaults to 0 -- equivalent to clientSignatures not in use.
  uint64_t num_clients; //defaults to 0; Load does nothing.
};

#endif
