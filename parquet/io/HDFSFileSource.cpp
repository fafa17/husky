//
// Created by Chris Liu on 15/12/2016.
//

#include <sstream>
#include <parquet/exception.h>
#include "HDFSFileSource.hpp"

void HDFSFileSource::Open(hdfsFS& fs, const std::string& path) {
    path_ = path;
    fs_ = fs;
    file_ = hdfsOpenFile(fs, path.c_str(), O_RDONLY,0 ,0 ,0);

    is_open_ = true;
    Seek(0);
    size_ = HDFSFileSource::Tell();
    Seek(0);
}

void HDFSFileSource::Seek(int64_t pos) {

    if (0 != hdfsSeek(fs_, file_, pos)) {
        std::stringstream ss;
        ss << "File seek to position " << pos << " failed.";
        throw ParquetException(ss.str());
    }
}

void HDFSFileSource::Close() {
    // Pure virtual
    CloseFile();
}

void HDFSFileSource::CloseFile() {
    if (is_open_) {
        hdfsCloseFile(fs_, file_);
        is_open_ = false;
    }
}

int64_t HDFSFileSource::Tell() const {
    int64_t position = hdfsTell(fs_, file_);
    if (position < 0) { throw ParquetException("ftell failed, did the file disappear?"); }
    return position;
}

int64_t HDFSFileSource::Read(int64_t nbytes, uint8_t* buffer) {
    return hdfsRead(fs_, file_, buffer, nbytes);
}

std::shared_ptr<Buffer> HDFSFileSource::Read(int64_t nbytes) {
    auto result = std::make_shared<OwnedMutableBuffer>(0, allocator_);
    result->Resize(nbytes);

    int64_t bytes_read = Read(nbytes, result->mutable_data());
    if (bytes_read < nbytes) { result->Resize(bytes_read); }
    return result;
}





