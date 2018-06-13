/**
 *    Copyright (C) 2017 HUAWEI Inc.
 *
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

#pragma once

#include <iostream>
#include <string>

#include "mongo/base/string_data.h"
#include "mongo/bson/util/builder.h"


namespace mongo {

class NamespaceString;


// Representation of a chunk identifier.

class ChunkId {
public:
    static const char kChunkIdField[];

    friend std::ostream& operator<<(std::ostream&, const ChunkId&);

    ChunkId() = default;

    ChunkId(const std::string& chunkId) : _chunkId(chunkId) {}

    ChunkId(const std::string&& chunkId) : _chunkId(std::move(chunkId)) {}

    operator StringData();

    bool operator==(const ChunkId&) const;
    bool operator!=(const ChunkId&) const;
    bool operator==(const std::string&) const;
    bool operator!=(const std::string&) const;

    bool operator<(const ChunkId&) const;

    const std::string& toString() const {
        return _chunkId;
    }

    int compare(const ChunkId& other) const;

    bool isValid() const {
        return !_chunkId.empty();
    }

    void appendForCommands(BSONObjBuilder* builder) const;

private:
    std::string _chunkId;
};

}  // namespace mongo

