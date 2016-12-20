//
// Created by Chris Liu on 15/12/2016.
//

#ifndef PROJECT_HDFSFILESOURCE_HPP
#define PROJECT_HDFSFILESOURCE_HPP

#include <parquet/util/mem-allocator.h>
#include <parquet/util/buffer.h>
#include <string>
#include <hdfs/hdfs.h>
#include <parquet/util/input.h>

using namespace parquet;

class HDFSFileSource : public RandomAccessSource {
public:
  explicit HDFSFileSource(MemoryAllocator* allocator = default_allocator())
      : file_(NULL), fs_(NULL), is_open_(false), allocator_(allocator) {}

    ~HDFSFileSource(){}

  void Open(hdfsFS&, const std::string& path);

  void Close();
  int64_t Tell() const;
  void Seek(int64_t pos);

  // Returns actual number of bytes read
  int64_t Read(int64_t nbytes, uint8_t* out);

  std::shared_ptr<Buffer> Read(int64_t nbytes);

  bool is_open() const { return is_open_; }
  const std::string& path() const { return path_; }

 protected:
  void CloseFile();

  std::string path_;
  hdfsFS fs_;
  hdfsFile file_;
  bool is_open_;
  MemoryAllocator* allocator_;
};


#endif //PROJECT_HDFSFILESOURCE_HPP
