// @file version.cpp

/*    Copyright 2009 10gen Inc.
 *
 *    Licensed under the Apache License, Version 2.0 (the "License");
 *    you may not use this file except in compliance with the License.
 *    You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 *    Unless required by applicable law or agreed to in writing, software
 *    distributed under the License is distributed on an "AS IS" BASIS,
 *    WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 *    See the License for the specific language governing permissions and
 *    limitations under the License.
 */

#include "pch.h"
#include <cstdlib>
#include <iostream>
#include <iomanip>
#include <sstream>
#include <string>
#include "version.h"

namespace mongo {

    const char versionString[] = "1.7.4-pre-";

    string mongodVersion() {
        stringstream ss;
        ss << "db version v" << versionString << ", pdfile version " << VERSION << "." << VERSION_MINOR;
        return ss.str();
    }

#ifndef _SCONS
    // only works in scons
    const char * gitVersion(){ return "not-scons"; }
#endif

    void printGitVersion() { log() << "git version: " << gitVersion() << endl; }

#ifndef _SCONS
#if defined(_WIN32)
    string sysInfo(){ 
        stringstream ss;
        ss << "not-scons win";
        ss << " mscver:" << _MSC_FULL_VER << " built:" << __DATE__;
        ss << " boostver:" << BOOST_VERSION;
#if( !defined(_MT) )
#error _MT is not defined
#endif
        ss << (sizeof(char *) == 8) ? " 64bit" : " 32bit";
        return ss.str();
    }
#else
    string sysInfo(){ return ""; }
#endif
#endif

    void printSysInfo() { 
        log() << "sys info: " << sysInfo() << endl; 
#if defined(_DURABLE)
        log() << "_DURABLE defined, but durable is not finished" << endl;
#endif
#if defined(_TESTINTENT)
        log() << "_TESTINTENT defined - this mode is for qa purposes" << endl;
#endif
    }

    //
    // 32 bit systems warning
    //
    void show_warnings(){
        // each message adds a leading but not a trailing newline

        bool warned = false;
        {
            const char * foo = strchr( versionString , '.' ) + 1;
            int bar = atoi( foo );
            if ( ( 2 * ( bar / 2 ) ) != bar ) {
                cout << "\n** NOTE: This is a development version (" << versionString << ") of MongoDB.";
                cout << "\n**       Not recommended for production." << endl;
                warned = true;
            }
        }

        if ( sizeof(int*) == 4 ) {
            cout << endl;
            cout << "** NOTE: when using MongoDB 32 bit, you are limited to about 2 gigabytes of data" << endl;
            cout << "**       see http://blog.mongodb.org/post/137788967/32-bit-limitations" << endl;
            warned = true;
        }

#ifdef __linux__
        if (boost::filesystem::exists("/proc/vz") && !boost::filesystem::exists("/proc/bc")){
            cout << endl;
            cout << "** WARNING: You are running in OpenVZ. This is known to be broken!!!" << endl;
            warned = true;
        }
#endif

        if (warned)
            cout << endl;
    }

}
