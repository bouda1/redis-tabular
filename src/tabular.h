#ifndef __TABULAR_H__
#define __TABULAR_H__
/*
** BSD 3-Clause License
**
** Copyright (c) 2018, David Boucher
** All rights reserved.
**
** Redistribution and use in source and binary forms, with or without
** modification, are permitted provided that the following conditions are met:
**
** * Redistributions of source code must retain the above copyright notice,
**   this list of conditions and the following disclaimer.
**
** * Redistributions in binary form must reproduce the above copyright notice,
**   this list of conditions and the following disclaimer in the documentation
**   and/or other materials provided with the distribution.
**
** * Neither the name of the copyright holder nor the names of its
**   contributors may be used to endorse or promote products derived from
**   this software without specific prior written permission.
**
** THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS "AS IS"
** AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT LIMITED TO, THE
** IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR A PARTICULAR PURPOSE
** ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT HOLDER OR CONTRIBUTORS BE
** LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL, SPECIAL, EXEMPLARY, OR
** CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT LIMITED TO, PROCUREMENT OF
** SUBSTITUTE GOODS OR SERVICES; LOSS OF USE, DATA, OR PROFITS; OR BUSINESS
** INTERRUPTION) HOWEVER CAUSED AND ON ANY THEORY OF LIABILITY, WHETHER IN
** CONTRACT, STRICT LIABILITY, OR TORT (INCLUDING NEGLIGENCE OR OTHERWISE)
** ARISING IN ANY WAY OUT OF THE USE OF THIS SOFTWARE, EVEN IF ADVISED OF THE
** POSSIBILITY OF SUCH DAMAGE.
*/
#include "redismodule.h"

enum _TabularFilter {
  TABULAR_SORT = 1 << 0,
  TABULAR_STORE = 1 << 1,
  TABULAR_FILTER = 1 << 2,
};

enum _TabularTool {
  TABULAR_NONE,
  TABULAR_MATCH,
  TABULAR_EQUAL,
  TABULAR_IN,
};

typedef enum _TabularTool TabularTool;

struct _TabularHeader {
    RedisModuleString *field;
    char type;
    const char *search;
    TabularTool tool;
};

typedef struct _TabularHeader TabularHeader;

void Swap(RedisModuleString **array, int block_size, int i, int j);
TabularHeader *ParseArgv(RedisModuleString **argv, int argc, int *size,
                         RedisModuleString **key_store, int flag);

#endif /*__TABULAR_H__*/
