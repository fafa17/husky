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

        class Row{
        public:
            std::shared_ptr<Field> fields;
        };

        class Field{
        public :
            std::shared_ptr<bool> getBoolean(){
                return (std::shared_ptr<bool>) value;
            }
            std::shared_ptr<uint8_t> getUint8(){
                return (std::shared_ptr<uint8_t>) value;
            }
            std::shared_ptr<int8_t> getInt8(){
                return (std::shared_ptr<int8_t>)value;
            }
            std::shared_ptr<uint16_t> getUint16(){
                return (std::shared_ptr<uint16_t>)value;
            }
            std::shared_ptr<int16_t> getInt16(){
                return (std::shared_ptr<int16_t>)value;
            }
            std::shared_ptr<uint32_t> getUint32(){
                return (std::shared_ptr<uint32_t>)value;
            }
            std::shared_ptr<int32_t> getInt32(){
                return (std::shared_ptr<int32_t>)value;
            }
            std::shared_ptr<uint64_t> getUint64(){
                return (std::shared_ptr<u_int64_t>)value;
            }
            std::shared_ptr<int64_t> getInt64(){
                return (std::shared_ptr<int64_t>)value;
            }
            std::shared_ptr<float_t> getFloat(){
                return (std::shared_ptr<float_t>)value;
            }
            std::shared_ptr<double_t> getDouble(){
                return (std::shared_ptr<double_t>)value;
            }
            std::shared_ptr<std::string> getString() {
                return (std::shared_ptr<std::string>) value;
            }
        private :
            std::shared_ptr value;
        };

        // Create attr list with row group
        class ParquetInputFormat : public InputFormatBase {
        public:
            ParquetInputFormat();
            ~ParquetInputFormat();
            typedef std::vector RecordT;
            bool next(RecordT&);

        protected:
            parquet::SchemaDescriptor &schema;
            void read_schema();
            void handle_next_row_group(long, long);
            bool fetch_new_row_group();
        };

//        // Create attr list with row group
//        class ParquetVectorizedInputFormat : public InputFormatBase {
//        public:
//            ParquetVectorizedInputFormat();
//            ~ParquetVectorizedInputFormat();
//            typedef std::vector RecordT;
//            bool next(RecordT&);
//
//        protected:
//            parquet::SchemaDescriptor &schema;
//            void read_schema();
//            void handle_next_row_group(long, long);
//            bool fetch_new_row_group();
//        };

        class ParquetRowGroupReader {
        public :
            ParquetRowGroupReader();
            ~ParquetRowGroupReader();
            void setStartEnd(long, long);
            bool nextColumn(shared_ptr<>)

        protected:
            std::unique_ptr<PageReader> currentPageReader;

        };


    }  // namespace io
}  // namespace husky

#endif //PROJECT_PARQUET_READER_HPP
