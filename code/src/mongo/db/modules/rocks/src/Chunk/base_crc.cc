
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <memory>
#include <string>
#include <sys/stat.h>
#include <sys/time.h>
#include <pthread.h>
#include <stdarg.h>
#include <unistd.h>
#include <sys/syscall.h>
#include <signal.h>
#include <time.h>
#include <atomic>

#include <mutex>
#include <thread>

#ifdef __cplusplus
#if __cplusplus
extern "C"{
#endif
#endif

using namespace std;

// metadata online check : tlv crc
#define POLY        0x82f63b78
#define CRC_LONG    8192
#define LONGx1      "8192"
#define LONGx2      "16384"
#define CRC_SHORT   256
#define SHORTx1     "256"
#define SHORTx2     "512"

static uint32_t crc32c_long[4][256];
static uint32_t crc32c_short[4][256];


/*****************************************************************************
*   Prototype    : IndexGF2MatrixTimes
*   Description  : Init GF2 Matrix
*   Input        : uint32_t *mator
*                  uint32_t vecor
*   Output       : None
*   Return Value : static inline uint32_t
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
static inline uint32_t IndexGF2MatrixTimes(uint32_t *mator, uint32_t vecor)
{
    uint32_t sumor = 0;

    while (vecor)
    {
        if (vecor & 1)
        {
            sumor ^= *mator;
        }
        vecor >>= 1; //lint !e702
        mator++;
    }
    return sumor;
}

/*****************************************************************************
*   Prototype    : IndexGF2MatrixSquare
*   Description  : GF2 matrix square
*   Input        : uint32_t *square
*                  uint32_t *mator
*   Output       : None
*   Return Value : static inline void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
static inline void IndexGF2MatrixSquare(uint32_t *square, uint32_t *mator)
{
    int i;
    for (i = 0; i < 32; i++)
    {
        square[i] = IndexGF2MatrixTimes(mator, mator[i]);
    }
}

/*****************************************************************************
*   Prototype    : IndexCRC32ZerosOp
*   Description  : crc zero operation
*   Input        : uint32_t *even
*                  uint64_t len
*   Output       : None
*   Return Value : static void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
static void IndexCRC32ZerosOp(uint32_t *even, uint64_t len)
{
    int i;
    uint32_t j;
    uint32_t data[32];

    data[0] = POLY;
    j = 1;
    for (i = 1; i < 32; i++)
    {
        data[i] = j;
        j <<= 1; //lint !e702
    }

    IndexGF2MatrixSquare(even, data);
    IndexGF2MatrixSquare(data, even);

    do
    {
        IndexGF2MatrixSquare(even, data);
        len >>= 1; //lint !e702
        if (len == 0)
        {
            return;
        }
        IndexGF2MatrixSquare(data, even);
        len >>= 1; //lint !e702
    } while (len);

    for (i = 0; i < 32; i++)
    {
        even[i] = data[i];
    }
}

/*****************************************************************************
*   Prototype    : IndexCRC32Zeros
*   Description  : crc zero
*   Input        : uint32_t data[][256]
*                  uint64_t len
*   Output       : None
*   Return Value : static void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
static void IndexCRC32Zeros(uint32_t data[][256], uint64_t len)
{
    uint32_t i;
    uint32_t opdata[32];
    IndexCRC32ZerosOp(opdata, len);
    for (i = 0; i < 256; i++)
    {
        data[0][i] = IndexGF2MatrixTimes(opdata, i);
        data[1][i] = IndexGF2MatrixTimes(opdata, i << 8);
        data[2][i] = IndexGF2MatrixTimes(opdata, i << 16);
        data[3][i] = IndexGF2MatrixTimes(opdata, i << 24);
    }
}

/*****************************************************************************
*   Prototype    : IndexCRC32Shift
*   Description  : crc shift
*   Input        : uint32_t data[][256]
*                  uint32_t crc
*   Output       : None
*   Return Value : static inline uint32_t
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
static inline uint32_t IndexCRC32Shift(uint32_t data[][256], uint32_t crc)
{
    return data[0][crc & 0xff] ^
        data[1][(crc >> 8) & 0xff] ^
        data[2][(crc >> 16) & 0xff] ^
        data[3][crc >> 24];
}

/*****************************************************************************
*   Prototype    : IndexCRC32Init
*   Description  : crc init
*   Input        : None
*   Output       : None
*   Return Value : void
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
void IndexCRC32Init()
{
    IndexCRC32Zeros(crc32c_long, CRC_LONG);
    IndexCRC32Zeros(crc32c_short, CRC_SHORT);
}

/*****************************************************************************
*   Prototype    : IndexCRC32
*   Description  : crc calc
*   Input        : uint32_t crc
*                  char *buf
*                  uint64_t length
*   Output       : None
*   Return Value : uint32_t
*   Calls        :
*   Called By    :
*
*   History:
*
*       1.  Date         : 2017/7/18
*           Author       : w00416554
*           Modification : Created function
*
*****************************************************************************/
uint32_t IndexCRC32(uint32_t crc, char *buf, uint64_t length)
{
    uint64_t crca, crcb, crcc;
    const unsigned char *end;
    const unsigned char *next = (unsigned char *)buf;

    crca = crc ^ 0xffffffff;
    while (length && ((uintptr_t)next & 7) != 0)
    {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crca)
                : "r"(next), "0"(crca));
        next++;
        length--;
    }

    while (length >= CRC_LONG * 3)
    {
        crcb = 0;
        crcc = 0;
        end = next + CRC_LONG;
        do
        {
            __asm__("crc32q\t" "(%3), %0\n\t"
                    "crc32q\t" LONGx1 "(%3), %1\n\t"
                    "crc32q\t" LONGx2 "(%3), %2"
                    : "=r"(crca), "=r"(crcb), "=r"(crcc)
                    : "r"(next), "0"(crca), "1"(crcb), "2"(crcc));
            next += 8;
        } while (next < end);
        crca = IndexCRC32Shift(crc32c_long, crca) ^ crcb;
        crca = IndexCRC32Shift(crc32c_long, crca) ^ crcc;
        next += CRC_LONG * 2;
        length -= CRC_LONG * 3;
    }

    while (length >= CRC_SHORT * 3)
    {
        crcb = 0;
        crcc = 0;
        end = next + CRC_SHORT;
        do
        {
            __asm__("crc32q\t" "(%3), %0\n\t"
                    "crc32q\t" SHORTx1 "(%3), %1\n\t"
                    "crc32q\t" SHORTx2 "(%3), %2"
                    : "=r"(crca), "=r"(crcb), "=r"(crcc)
                    : "r"(next), "0"(crca), "1"(crcb), "2"(crcc));
            next += 8;
        } while (next < end);
        crca = IndexCRC32Shift(crc32c_short, crca) ^ crcb;
        crca = IndexCRC32Shift(crc32c_short, crca) ^ crcc;
        next += CRC_SHORT * 2;
        length -= CRC_SHORT * 3;
    }

    end = next + (length - (length & 7));
    while (next < end)
    {
        __asm__("crc32q\t" "(%1), %0"
                : "=r"(crca)
                : "r"(next), "0"(crca));
        next += 8;
    }
    length &= 7;

    while (length)
    {
        __asm__("crc32b\t" "(%1), %0"
                : "=r"(crca)
                : "r"(next), "0"(crca));
        next++;
        length--;
    }

    return (uint32_t)crca ^ 0xffffffff;
}

#ifdef __cplusplus
#if __cplusplus
}
#endif
#endif /* __cplusplus */
