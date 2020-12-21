# Copyright 2015 MongoDB Inc.
#
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
#
# http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

import SCons
import gzip
import shutil



def open_file_gziptool(file_name, mode='r', encoding=None, **kwargs):
    if mode in ['r', 'rt', 'tr'] and encoding is None:
        with open(file_name, 'rb') as f:
            context = f.read()
            for encoding_item in ['UTF-8', 'GBK', 'ISO-8859-1']:
                try:
                    context.decode(encoding=encoding_item)
                    encoding = encoding_item
                    break
                except UnicodeDecodeError as e:
                    pass
    return open(file_name, mode=mode, encoding=encoding, **kwargs)

def GZipAction(target, source, env, **kw):
    dst_gzip = gzip.GzipFile(str(target[0]), 'wb')
    with open_file_gziptool(str(source[0]), 'r') as src_file:
        shutil.copyfileobj(src_file, dst_gzip)
    dst_gzip.close()

def generate(env, **kwargs):
    env['BUILDERS']['__GZIPTOOL'] = SCons.Builder.Builder(
        action=SCons.Action.Action(GZipAction, "$GZIPTOOL_COMSTR")
    )
    env['GZIPTOOL_COMSTR'] = kwargs.get(
        "GZIPTOOL_COMSTR",
        "Compressing $TARGET with gzip"
    )

    def GZipTool(env, target, source):
        result = env.__GZIPTOOL(target=target, source=source)
        env.AlwaysBuild(result)
        return result

    env.AddMethod(GZipTool, 'GZip')

def exists(env):
    return True
