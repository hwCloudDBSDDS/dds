/**
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
 *    must comply with the GNU Affero General Public License in all respects for
 *    all of the code used other than as permitted herein. If you modify file(s)
 *    with this exception, you may extend this exception to your version of the
 *    file(s), but you are not obligated to do so. If you do not wish to do so,
 *    delete this exception statement from your version. If you delete this
 *    exception statement from all source files in the program, then also delete
 *    it in the license file.
 */

#include "mongo/platform/basic.h"

#include <functional>
#include <string.h>

#include "mongo/base/status_with.h"
#include "mongo/db/jsobj.h"
#include "mongo/s/chunk_id.h"

namespace mongo {

using std::string;
using std::ostream;

const char ChunkId::kChunkIdField[] = "chunkId";

bool ChunkId::operator==(const ChunkId& other) const {
    return (this->_chunkId == other._chunkId);
}

bool ChunkId::operator!=(const ChunkId& other) const {
    return !(*this == other);
}

bool ChunkId::operator==(const string& other) const {
    return (this->_chunkId == other);
}

bool ChunkId::operator!=(const string& other) const {
    return !(*this == other);
}

ChunkId::operator StringData() {
    return StringData(_chunkId.data(), _chunkId.size());
}

ostream& operator<<(ostream& os, const ChunkId& chunkId) {
    os << chunkId._chunkId;
    return os;
}

bool ChunkId::operator<(const ChunkId& other) const {
    return _chunkId < other._chunkId;
}

int ChunkId::compare(const ChunkId& other) const {
    return _chunkId.compare(other._chunkId);
}

void ChunkId::appendForCommands(BSONObjBuilder* builder) const {
    builder->append(kChunkIdField, toString());
}

}  // namespace mongo
