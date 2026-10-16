
#ifndef RINGORAM_HELPER_H
#define RINGORAM_HELPER_H

#include "RingORAM.h"
#include "NetIOConnector.h"
#include <vector>
#include <string>
#include <cassert>

using namespace std;
using namespace CryptoPP;

void test_ringoram(uint32_t, std::vector<std::string> &, uint32_t, RingORAM*, size_t);
void testTable(string, string, NetIOConnector *);

#endif //RINGORAM_HELPER_H
