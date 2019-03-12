/*
 *    This program is free software: you can redistribute it and/or  modify
 *    it under the terms of the GNU Affero General Public License, version 3,
 *    as published by the Free Software Foundation.
 *
 *    This program is distributed in the hope that it will be useful,
 *    but WITHOUT ANY WARRANTY; without even the implied warranty of
 *    MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 *    GNU Affero General Public License for more details.
 *
 *    You should have received a copy of the GNU Affero General Public License
 *    along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 *    As a special exception, the copyright holders give permission to link the
 *    code of portions of this program with the OpenSSL library under certain
 *    conditions as described in each individual source file and distribute
 *    linked combinations including the program with the OpenSSL library. You
 *    must comply with the GNU Affero General Public License in all respects
 *    for all of the code used other than as permitted herein. If you modify
 *    file(s) with this exception, you may extend this exception to your
 *    version of the file(s), but you are not obligated to do so. If you do not
 *    wish to do so, delete this exception statement from your version. If you
 *    delete this exception statement from all source files in the program,
 *    then also delete it in the license file.
 */

#include "mongo/s/chunk_id.h"

#include "mongo/base/string_data.h"
#include "mongo/platform/basic.h"
#include "mongo/unittest/unittest.h"

namespace mongo {
namespace {

using std::string;
using namespace mongo;

TEST(ChunkId, Valid) {
    ChunkId chunkId("my_chunk_id");
    ASSERT(chunkId.isValid());
}

TEST(ChunkId, Invalid) {
    ChunkId chunkId("");
    ASSERT(!chunkId.isValid());
}

TEST(ChunkId, Roundtrip) {
    string chunk_id_str("my_chunk_id");
    ChunkId chunkId(chunk_id_str);
    ASSERT(chunk_id_str == chunkId.toString());
}

TEST(ChunkId, ToStringData) {
    string chunk_id_str("my_chunk_id");
    ChunkId chunkId(chunk_id_str);
    StringData stringData(chunkId);
    ASSERT(stringData == chunk_id_str);
}

TEST(ChunkId, Compare) {
    string a("aaa");
    string a1("aaa");
    string b("bbb");
    ChunkId sa(a);
    ChunkId sa1(a1);
    ChunkId sb(b);
    ASSERT_EQUALS(sa.compare(sa1), a.compare(a1));
    ASSERT_EQUALS(sb.compare(sa1), b.compare(a1));
    ASSERT_EQUALS(sa.compare(sb), a.compare(b));
}

}  // namespace
}  // namespace mongo
