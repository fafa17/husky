//
// Steps:
//      1. get Footer and metadata
//      2. Cal the split
//      3. Ask wokrer to read the split
//      4.

#ifndef PROJECT_PARQUET_READER_HPP
#define PROJECT_PARQUET_READER_HPP


#include <parquet/model/Field.hpp>
#include "io/input/file_inputformat_impl.hpp"

#include "parquet/file/metadata.h"
#include "parquet/schema/descriptor.h"
#include "parquet/file/reader.h"
#include "parquet/model/Field.hpp"
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

//        parquet::FileMetaData read_footer(bool skipRowGroup, const std::string &url);
//
//        class ParquetLoader {
//        public:
//            ParquetLoader();
//            ~ParquetLoader();
//            bool load(const std::string& url);
//
//        protected:
//            hdfsFS& hdfs;
//        };

        class Row{
        public:
            Row();
            void set(std::shared_ptr<Field[]> fields){
                this->fields = fields;
            }
        private:
            std::shared_ptr<Field[]> fields;
        };


        // Create attr list with row group
        class ParquetInputFormat : public InputFormatBase {
        public:
            ParquetInputFormat();
            ~ParquetInputFormat();

            typedef std::shared_ptr<Row> RecordT;
            bool next(RecordT&);

            void set(std::string, int64_t , int64_t);

            bool is_setup() const {
                return isSetup;
            }

            int32_t getNumOfColumn(){schema->num_columns();}
            int64_t getNumOfRow(){current_row_group_reader->metadata()->num_rows();}

        protected:
            const parquet::SchemaDescriptor* schema;

            bool isSetup = false;

            std::string current_file;
            int64_t current_start_pos;
            int64_t current_len;

            void convertToRow();
            std::shared_ptr<Row[]> row_buffer;

            std::unique_ptr<parquet::ParquetFileReader> current_file_reader;
            std::shared_ptr<parquet::RowGroupReader> current_row_group_reader;

        };

    }  // namespace io
}  // namespace husky

#endif //PROJECT_PARQUET_READER_HPP
