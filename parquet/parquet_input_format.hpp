//
// Steps:
//      1. get Footer and metadata
//      2. Cal the split
//      3. Ask wokrer to read the split
//      4.

#ifndef PROJECT_PARQUET_READER_HPP
#define PROJECT_PARQUET_READER_HPP


#include "io/input/file_inputformat_impl.hpp"

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
    namespace io {

        parquet::FileMetaData read_footer(bool skipRowGroup, const std::string &url)

        class ParquetLoader {
        public:
            ParquetLoader();
            ~ParquetLoader();
            bool load(const std::string &url);

        protected:
            void read_schema();
            void read_row_group_metadata();
            void set_assigner();
        };

        class ParquetInputFormat{
        public:
            ParquetReader();
            ~ParquetReader();
            next();

        protected:
            SchemaDescriptor &schema;
            void read_schema();
            void handle_next_row_group(long start, long len);
            bool fetch_new_row_group();
        };
    }  // namespace io
}  // namespace husky

#endif //PROJECT_PARQUET_READER_HPP
