/*
 *  Copyright 2016 by SEAL-ORAM Project
 *
 *  Licensed under the Apache License, Version 2.0 (the "License");
 *  you may not use this file except in compliance with the License.
 *  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *  Unless required by applicable law or agreed to in writing, software
 *  distributed under the License is distributed on an "AS IS" BASIS,
 *  WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *  See the License for the specific language governing permissions and
 *  limitations under the License.
 */

#include "Util.h"
#include <iostream>

using namespace CryptoPP;

size_t Util::key_length = (size_t)AES::DEFAULT_KEYLENGTH;
size_t Util::aes_block_size = (size_t)AES::BLOCKSIZE;
AutoSeededRandomPool Util::prng;
CFB_Mode<AES>::Encryption Util::encrypt_handler;
CFB_Mode<AES>::Decryption Util::decrypt_handler;
std::random_device Util::rd;
std::mt19937 Util::gen(Util::rd());

// THREAD SAFETY (fixed 9/22/26): the cipher objects and the PRNG used to be
// the shared statics Util::encrypt_handler / Util::prng, and every call did
// SetKeyWithIV on them.  Two threads encrypting at the same time therefore
// clobbered each other's key/IV state: a stress test of 8 threads doing 16000
// round-trips produced wrong plaintext.  It did not bite in production only
// because each ORAM runs in its own FORKED CHILD, so the statics were not
// actually shared — any move to threads would have corrupted data silently.
// Each thread now gets its own handler and PRNG; the class statics stay for
// source compatibility.
void Util::aes_encrypt(const std::string& plain, const byte* key, std::string& cipher) {
    static thread_local CFB_Mode<AES>::Encryption enc;
    static thread_local AutoSeededRandomPool rng;

    byte iv[AES::BLOCKSIZE];
    enc.GetNextIV(rng, iv);
    enc.SetKeyWithIV(key, key_length, iv, aes_block_size);

    std::string cipher_text(plain.length(), '\0');
    if (!plain.empty())
        enc.ProcessData(reinterpret_cast<byte*>(&cipher_text[0]),
                        reinterpret_cast<const byte*>(plain.data()), plain.length());
    cipher.assign(reinterpret_cast<const char*>(iv), aes_block_size);
    cipher += cipher_text;
}

void Util::aes_decrypt(const std::string& cipher, const byte* key, std::string& plain) {
    static thread_local CFB_Mode<AES>::Decryption dec;

    // A ciphertext shorter than one IV is not decryptable.  The old code
    // computed cipher.length() - aes_block_size in SIZE_T, so an empty or
    // truncated buffer wrapped around to ~2^64 and the stack array below
    // segfaulted.  That is reachable: the storage server answers a missing
    // slot with a short/zero buffer.  Fail loudly and leave `plain` empty.
    if (cipher.length() < aes_block_size) {
        plain.clear();
        std::cerr << "[Util::aes_decrypt] ciphertext is " << cipher.length()
                  << " bytes, needs at least " << aes_block_size
                  << " (IV).  Slot missing or wrong block_size?" << std::endl;
        return;
    }

    dec.SetKeyWithIV(key, key_length, reinterpret_cast<const byte*>(cipher.data()), aes_block_size);
    const size_t cipher_length = cipher.length() - aes_block_size;

    plain.assign(cipher_length, '\0');
    if (cipher_length)
        dec.ProcessData(reinterpret_cast<byte*>(&plain[0]),
                        reinterpret_cast<const byte*>(cipher.data()) + aes_block_size,
                        cipher_length);
}

std::string Util::sha256_hash(const std::string& key, const std::string& salt) {
    byte buf[SHA256::DIGESTSIZE];
    SHA256().CalculateDigest(buf, (byte*) ((key + salt).c_str()), key.length() + salt.length());
    return std::string((const char*)buf, (size_t)SHA256::DIGESTSIZE);
}

std::string Util::generate_random_block(const size_t& length) {
    static thread_local AutoSeededRandomPool rng;   // see aes_encrypt
    if (!length) return {};
    std::string buf(length, '\0');                 // heap, not a VLA: `length`
                                                   // is a run-time value and
                                                   // byte buf[length] is both
                                                   // non-standard C++ and a
                                                   // stack-overflow risk.
    rng.GenerateBlock(reinterpret_cast<byte*>(&buf[0]), length);
    return buf;
}