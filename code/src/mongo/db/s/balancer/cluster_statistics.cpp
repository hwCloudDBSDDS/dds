/**
 *    Copyright (C) 2016 MongoDB Inc.
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

#include "mongo/platform/basic.h"

#include "mongo/db/s/balancer/cluster_statistics.h"

#include "mongo/bson/bsonobj.h"
#include "mongo/bson/bsonobjbuilder.h"
#include "mongo/bson/util/bson_extract.h"

namespace mongo {

ClusterStatistics::ClusterStatistics() = default;

ClusterStatistics::~ClusterStatistics() = default;

bool ClusterStatistics::ChunkStatistics::isAboveSplitThreshold(uint64_t splitThreshold) const {
    // TODO:  caculate according to the statistics
    return (sstfilesize * 1024 * 1024) > splitThreshold;
}

bool ClusterStatistics::ChunkStatistics::isUnderSplitThreshold(uint64_t splitThreshold) const {
    // TODO:  caculate according to the statistics
    return false;
}

BSONObj ClusterStatistics::ChunkStatistics::toBSON() const {
    BSONObjBuilder builder;
    builder.append("id", chunkId.toString());
    builder.append("currSizeMB", static_cast<long long>(currSizeMB));
    builder.append("documentNum", static_cast<long long>(documentNum));
    builder.append("tps", static_cast<long long>(tps));
    builder.append("sstfileNum", static_cast<long long>(sstfileNum));
    builder.append("sstfilesize", static_cast<long long>(sstfilesize));

    return builder.obj();
}

StatusWith<ClusterStatistics::ChunkStatistics> ClusterStatistics::ChunkStatistics::fromBSON(const BSONObj& source) {
    ClusterStatistics::ChunkStatistics chunkStatistics;

    {
        std::string ns;
        Status status = bsonExtractStringField(source, "ns", &ns);
        if (!status.isOK()) {
            return status;
        }
        chunkStatistics.ns = ns;
    }

    {
        std::string chunkIdstr;
        Status status = bsonExtractStringField(source, "chunkid", &chunkIdstr);
        if (!status.isOK()) {
            return status;
        }
        ChunkId chunkId(chunkIdstr);
        chunkStatistics.chunkId = chunkId;
    }

    {
        long long size;
        Status status = bsonExtractIntegerField(source, "size", &size);
        if (!status.isOK()) {
            return status;
        }
        chunkStatistics.currSizeMB = size;
    }

    {
        long long numRecords;
        Status status = bsonExtractIntegerField(source, "count", &numRecords);
        if (!status.isOK()) {
            return status;
        }
        chunkStatistics.documentNum = numRecords;
    }

    {
        long long tps;
        Status status = bsonExtractIntegerField(source, "tps", &tps);
        if (!status.isOK()) {
            return status;
        }
        chunkStatistics.tps = tps;
    }

    {
        BSONElement rockdb;
        Status status = bsonExtractTypedField(source, "rocksdb", Object, &rockdb);
        if (status.isOK()) {
            BSONObj obj = rockdb.Obj();
            long long num;
            status = bsonExtractIntegerField(obj, "count", &num);
            if (status.isOK()) {
                chunkStatistics.sstfileNum = num;
            }
            long long size;
            status = bsonExtractIntegerField(obj, "size", &size);
            if (status.isOK()) {
                chunkStatistics.sstfilesize = size;
            }
        }
    }

    return chunkStatistics;
}

bool ClusterStatistics::ShardStatistics::isAboveThreshold() const {
    // TODO:  caculate according to the statistics
    return false;
}

BSONObj ClusterStatistics::ShardStatistics::toBSON() const {
    BSONObjBuilder builder;
    builder.append("id", shardId.toString());

    BSONArrayBuilder b;
    for (const ClusterStatistics::ChunkStatistics& chunkStat : chunkStatistics) {
        b << chunkStat.toBSON();
    }
    builder.append("chunks", b.arr());

    return builder.obj();
}

StatusWith<ClusterStatistics::ShardStatistics> ClusterStatistics::ShardStatistics::fromBSON(const BSONObj& source) {
    ClusterStatistics::ShardStatistics shardStatistics;

    // TODO: parse cpu and mem
    {
        std::string version;
        Status status = bsonExtractStringField(source, "mongoVersion", &version);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.mongoVersion = version;
    }            

    // TODO: parse cpu and mem
    {
        BSONElement cpuInfo;
        Status status = bsonExtractTypedField(source, "cpu", Object, &cpuInfo);
        if (!status.isOK()) {
            return status;
        }
    }

    {
        BSONElement memInfo;
        Status status = bsonExtractTypedField(source, "memory", Object, &memInfo);
        if (!status.isOK()) {
            return status;
        }
    }

    {
        BSONElement bandwidthInfo;
        Status status = bsonExtractTypedField(source, "bandwidth", Object, &bandwidthInfo);
        if (!status.isOK()) {
            return status;
        }

        BSONObj obj = bandwidthInfo.Obj();

        long long read;
        status = bsonExtractIntegerField(obj, "read", &read);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.netWorkInfo.readBandWidth = read;

        long long write;
        status = bsonExtractIntegerField(obj, "write", &write);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.netWorkInfo.writeBandWidth = write;
    }

    {
        BSONElement opsInfo;
        Status status = bsonExtractTypedField(source, "ops", Object, &opsInfo);
        if (!status.isOK()) {
            return status;
        }

        BSONObj obj = opsInfo.Obj();

        long long ops;
        status = bsonExtractIntegerField(obj, "insert", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.insertOps = ops;

        status = bsonExtractIntegerField(obj, "query", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.queryOps = ops;

        status = bsonExtractIntegerField(obj, "update", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.updateOps = ops;

        status = bsonExtractIntegerField(obj, "delete", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.deleteOps = ops;

        status = bsonExtractIntegerField(obj, "getmore", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.getmoreOps = ops;

        status = bsonExtractIntegerField(obj, "command", &ops);
        if (!status.isOK()) {
            return status;
        }
        shardStatistics.opsInfo.commandOps = ops;
    }

    {
        BSONElement chunksElem;
        Status status = bsonExtractTypedField(source, "chunks", Array, &chunksElem);
        if (status.isOK()) {
            BSONObjIterator it(chunksElem.Obj());
            while (it.more()) {
                auto chunkStatistics = ClusterStatistics::ChunkStatistics::fromBSON(it.next().Obj());
                if (!chunkStatistics.isOK()) {
                    return chunkStatistics.getStatus();
                }

                shardStatistics.chunkStatistics.push_back(chunkStatistics.getValue());
            }
        } else if (status == ErrorCodes::NoSuchKey) {

        } else {
            return status;
        }
    }

    return shardStatistics;
}

}  // namespace mongo
