#ifndef __INDEX_CRC_H__
#define __INDEX_CRC_H__

#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif /* __cplusplus */

// metadata online check : tlv crc
#define IndexOffsetOf(t, m) ((uint64_t)(&((t *)0)->m))
#define INDEX_CRC_INIT_VALUE        0xffffffff;

extern void IndexCRC32Init();
extern uint32_t IndexCRC32(uint32_t crc, char *buffer, uint64_t size);

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */

#endif

