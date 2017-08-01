/**
 * Copyright (C) 2017 MongoDB Inc.
 *
 * This program is free software: you can redistribute it and/or  modify
 * it under the terms of the GNU Affero General Public License, version 3,
 * as published by the Free Software Foundation.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU Affero General Public License for more details.
 *
 * You should have received a copy of the GNU Affero General Public License
 * along with this program.  If not, see <http://www.gnu.org/licenses/>.
 *
 * As a special exception, the copyright holders give permission to link the
 * code of portions of this program with the OpenSSL library under certain
 * conditions as described in each individual source file and distribute
 * linked combinations including the program with the OpenSSL library. You
 * must comply with the GNU Affero General Public License in all respects
 * for all of the code used other than as permitted herein. If you modify
 * file(s) with this exception, you may extend this exception to your
 * version of the file(s), but you are not obligated to do so. If you do not
 * wish to do so, delete this exception statement from your version. If you
 * delete this exception statement from all source files in the program,
 * then also delete it in the license file.
 */

#pragma once

#include "mongo/db/logical_session_id.h"

namespace mongo {

/**
 * Factory functions to generate logical session records.
 */
LogicalSessionId makeLogicalSessionId(const LogicalSessionFromClient& lsid,
                                      OperationContext* opCtx);
LogicalSessionId makeLogicalSessionId(OperationContext* opCtx);

/**
 * Factory functions to make logical session records. The overloads that
 * take an OperationContext should be used when possible, as they will also set the
 * user information on the record.
 */
LogicalSessionRecord makeLogicalSessionRecord(const LogicalSessionId& lsid, Date_t lastUse);
LogicalSessionRecord makeLogicalSessionRecord(OperationContext* opCtx, Date_t lastUse);
LogicalSessionRecord makeLogicalSessionRecord(OperationContext* opCtx,
                                              const LogicalSessionId& lsid,
                                              Date_t lastUse);

LogicalSessionToClient makeLogicalSessionToClient(const LogicalSessionId& lsid);

/**
 * Parses the session information from the body of a request and installs it on the current
 * operation context. Must only be called once per operation and should be done right in the
 * beginning.
 *
 * Throws if the sessionId/txnNumber combination is not properly formatted.
 *
 * requiresAuth specifies if the command we're initializing operationSessionInfo for requires
 * authorization or not.  This can be determined by invoking ->requiresAuth() on the parsed command.
 */
void initializeOperationSessionInfo(OperationContext* opCtx,
                                    const BSONObj& requestBody,
                                    bool requiresAuth);

}  // namespace mongo
