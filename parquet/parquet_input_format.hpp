//
// Steps:
//      1. get Footer and metadata
//      2. Cal the split
//      3. Ask wokrer to read the split
//      4.

#ifndef PROJECT_PARQUET_READER_HPP
#define PROJECT_PARQUET_READER_HPP

#include <parquet/types.h>
#include <parquet/file/metadata.h>
#include <parquet/schema/descriptor.h>
#include <parquet/file/reader.h>

#include "io/input/file_inputformat_impl.hpp"
#include "parquet/model/Field.hpp"
#include "parquet/model/Row.hpp"
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
    namespace pt{
        int GetTypeByteSize(parquet::Type::type parquet_type);
    }
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



        // Create attr list with row group
        class ParquetInputFormat : public InputFormatBase {
        public:
            ParquetInputFormat() = default;
            ~ParquetInputFormat() = default;

            typedef Row RecordT;
            bool next(RecordT&);

            void set_input(std::string &path){
                current_file = path;
                isSetup = true;
            }

            //TODO: TBD
            void setLocal(std::string, int64_t , int64_t);

            bool is_setup() const {
                return isSetup;
            }

            int32_t getNumOfColumn(){ return schema->num_columns(); }
            int64_t getNumOfRow(){ return current_row_group_reader->metadata()->num_rows(); }
            const parquet::SchemaDescriptor* getSchema() {return current_file_reader->metadata()->schema(); }
            bool nextRowGroup();

            void setRowGroup(hdfsFS, std::string, int32_t);

        protected:

            const parquet::SchemaDescriptor* schema;

            bool isSetup = false;

            //no more splits to be processed
            bool finish = false;

            std::string current_file;

            std::vector<Row>* row_buffer = nullptr;
            int row_buffer_counter = 0;

            void convertToRow();

            std::unique_ptr<::parquet::ParquetFileReader> current_file_reader;
            std::shared_ptr<::parquet::RowGroupReader> current_row_group_reader;

        };

    }  // namespace io
}  // namespace husky

#endif //PROJECT_PARQUET_READER_HPP
