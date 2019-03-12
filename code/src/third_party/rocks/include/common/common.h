//  Copyright (c) 2017-present.  All rights reserved.
//  This source code is licensed under the BSD-style license found in the
//  LICENSE file in the root directory of this source tree. An additional grant
//  of patent rights can be found in the PATENTS file in the same directory.

#ifndef STORAGE_ROCKSDB_INCLUDE_COMMON_H_
#define STORAGE_ROCKSDB_INCLUDE_COMMON_H_

#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif /* __cplusplus */

void*  __IndexMemAllocZero(uint32_t uiBytes);

void  __IndexMemFree(void *pvUsrMem);

#define CommonMemCopy(p_dst, dst_max, p_src, size)          memcpy((p_dst), (p_src), (size))
#define CommonMemZero(p_dst, size)                          memset((p_dst), 0, (size))
#define CommonStrCopy(p_dst, size, p_src)                   strcpy((p_dst), (p_src))
#define CommonSnprintf(p_dst,dst_max, size, format...)      snprintf((p_dst), (size), ##format)



#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif

