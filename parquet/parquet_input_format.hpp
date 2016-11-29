//
// Steps:
//      1. get Footer and metadata
//      2. Cal the split
//      3. Ask wokrer to read the split
//      4.

#ifndef PROJECT_PARQUET_READER_HPP
#define PROJECT_PARQUET_READER_HPP


#include "io/input/file_inputformat_impl.hpp"

#include "parquet/file/metadata.h"
#include "parquet/schema/descriptor.h"
/**
 * Loader
 * ------------
 * Build object list
 * Build all attr list accroding schema
 *
 * Reader
 * ------------
 * Given a map <string, attrList>
 * Executor read the
 */
namespace husky {
    namespace load{

    }
    namespace io {

        parquet::FileMetaData read_footer(bool skipRowGroup, const std::string &url);

        class ParquetLoader {
        public:
            ParquetLoader();
            ~ParquetLoader();
            bool load(const std::string& url);

        protected:
            hdfsFS& hdfs;
        };

        // Create attr list with row group
        class ParquetVectorizedInputFormat : public InputFormatBase {
        public:
            ParquetVectorizedInputFormat();
            ~ParquetVectorizedInputFormat();
            typedef std::vector RecordT;
            bool next(RecordT&);

        protected:
            parquet::SchemaDescriptor &schema;
            void read_schema();
            void handle_next_row_group(long, long);
            bool fetch_new_row_group();
        };
    }  // namespace io
}  // namespace husky

#endif //PROJECT_PARQUET_READER_HPP
