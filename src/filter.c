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
#include <fnmatch.h>
#include <string.h>
#include "filter.h"

/**
 *  Filter A multicolumn string filter function
 *
 * @param array The array to apply filter on.
 * @param size The array size
 * @param header Informations on each column, it contains the filter patterns
 * @param block_size The number of columns.
 *
 * @return The new size of the array. The real size of array is not modified
 *         to avoid reallocations, with this information, we know that the new
 *         array must be read in the range [0, return value).
 */
int Filter(RedisModuleCtx *ctx, RedisModuleString **array, int size,
           TabularHeader *header, int block_size) {
    int retval = size - block_size;
    int i, j;
    for (j = 0; j < block_size - 1; ++j) {
        size_t len;
        switch (header[j].tool) {
            case TABULAR_MATCH:
                i = 0;
                while (i <= retval) {
                    const char *txt = RedisModule_StringPtrLen(array[i + j], &len);
                    if (fnmatch(header[j].search, txt, FNM_NOESCAPE | FNM_CASEFOLD | FNM_EXTMATCH)) {
                        Swap(array, block_size, i, retval);
                        retval -= block_size;
                    }
                    else
                        i += block_size;
                }
                break;
            case TABULAR_EQUAL:
                i = 0;
                while (i <= retval) {
                    const char *txt = RedisModule_StringPtrLen(array[i + j], &len);
                    if (strncmp(header[j].search, txt, len)) {
                        Swap(array, block_size, i, retval);
                        retval -= block_size;
                    }
                    else
                        i += block_size;
                }
                break;
            case TABULAR_IN:
                i = 0;
                while (i <= retval) {
                    int v;
                    if (array[i + j]) {
                        RedisModuleCallReply *reply = RedisModule_Call(
                                ctx, "SISMEMBER", "cs",
                                header[j].search, array[i + j]);
                        v = RedisModule_CallReplyInteger(reply);
                        RedisModule_FreeCallReply(reply);
                    }
                    else
                        v = 0;

                    if (!v) {
                        Swap(array, block_size, i, retval);
                        retval -= block_size;
                    }
                    else
                        i += block_size;
                }
                break;
        }
    }
    return retval + block_size;
}
