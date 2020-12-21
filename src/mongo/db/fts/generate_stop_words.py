import sys



def open_file_generate_stop_words(file_name, mode='r', encoding=None, **kwargs):
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

def generate( header, source, language_files ):
    out = open_file_generate_stop_words( header, "w" )
    out.write( """
#pragma once
#include <set>
#include <string>
#include "mongo/util/string_map.h"
namespace mongo {
namespace fts {

  void loadStopWordMap( StringMap< std::set< std::string > >* m );
}
}
""" )
    out.close()



    out = open_file_generate_stop_words( source, "w", encoding='utf-8')
    out.write( '#include "%s"' % header.rpartition( "/" )[2].rpartition( "\\" )[2] )
    out.write( """
namespace mongo {
namespace fts {

  void loadStopWordMap( StringMap< std::set< std::string > >* m ) {

""" )

    for l_file in language_files:
        l = l_file.rpartition( "_" )[2].partition( "." )[0]

        out.write( '  // %s\n' % l_file )
        out.write( '  {\n' )
        out.write( '   const char* const words[] = {\n' )
        for word in open_file_generate_stop_words( l_file, "rb" ):
            out.write( '       "%s",\n' % word.decode('utf-8').strip() )
        out.write( '   };\n' )
        out.write( '   const size_t wordcnt = sizeof(words) / sizeof(words[0]);\n' )
        out.write( '   std::set< std::string >& l = (*m)["%s"];\n' % l )
        out.write( '   l.insert(&words[0], &words[wordcnt]);\n' )
        out.write( '  }\n' )
    out.write( """
  }
} // namespace fts
} // namespace mongo
""" )


if __name__ == "__main__":
    generate( sys.argv[ len(sys.argv) - 2],
              sys.argv[ len(sys.argv) - 1],
              sys.argv[1:-2] )
