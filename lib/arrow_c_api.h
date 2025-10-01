#ifndef __OCAML_ARROW_C_API__
#define __OCAML_ARROW_C_API__

#include<stdint.h>

#ifdef __cplusplus
#include<arrow/c/abi.h>
#include<arrow/c/bridge.h>
#include<arrow/api.h>
#include<arrow/csv/api.h>
#include<arrow/io/api.h>
#include<arrow/json/api.h>
#include<arrow/ipc/api.h>
#include<arrow/ipc/feather.h>
#include<arrow/util/bitmap_ops.h>
#include<parquet/arrow/reader.h>
#include<parquet/arrow/writer.h>
#include<parquet/exception.h>

typedef std::shared_ptr<arrow::Table> TablePtr;
typedef std::shared_ptr<arrow::ArrayBuilder> BuilderPtr;
typedef std::shared_ptr<arrow::StringBuilder> StringBuilderPtr;
typedef std::shared_ptr<arrow::Int32Builder> Int32BuilderPtr;
typedef std::shared_ptr<arrow::Int64Builder> Int64BuilderPtr;
typedef std::shared_ptr<arrow::DoubleBuilder> DoubleBuilderPtr;
typedef std::shared_ptr<arrow::FloatBuilder> FloatBuilderPtr;
typedef std::shared_ptr<arrow::BooleanBuilder> BooleanBuilderPtr;
typedef std::shared_ptr<arrow::UInt8Builder> UInt8BuilderPtr;
typedef std::shared_ptr<arrow::UInt16Builder> UInt16BuilderPtr;
typedef std::shared_ptr<arrow::UInt32Builder> UInt32BuilderPtr;
typedef std::shared_ptr<arrow::UInt64Builder> UInt64BuilderPtr;
typedef std::shared_ptr<arrow::Int8Builder> Int8BuilderPtr;
typedef std::shared_ptr<arrow::Int16Builder> Int16BuilderPtr;
typedef std::shared_ptr<arrow::ChunkedArray> ChunkedArrayPtr;
typedef std::shared_ptr<arrow::Date32Builder> Date32BuilderPtr;
typedef std::shared_ptr<arrow::Date64Builder> Date64BuilderPtr;
typedef std::shared_ptr<arrow::Time32Builder> Time32BuilderPtr;
typedef std::shared_ptr<arrow::Time64Builder> Time64BuilderPtr;
typedef std::shared_ptr<arrow::TimestampBuilder> TimestampBuilderPtr;
typedef std::shared_ptr<arrow::DurationBuilder> DurationBuilderPtr;

struct ParquetReader {
  std::unique_ptr<parquet::arrow::FileReader> reader;
  std::unique_ptr<arrow::RecordBatchReader> batch_reader;
};

extern "C" {
#else
typedef void TablePtr;
typedef void ParquetReader;
typedef void BuilderPtr;
typedef void StringBuilderPtr;
typedef void Int32BuilderPtr;
typedef void Int64BuilderPtr;
typedef void DoubleBuilderPtr;
typedef void FloatBuilderPtr;
typedef void BooleanBuilderPtr;
typedef void UInt8BuilderPtr;
typedef void UInt16BuilderPtr;
typedef void UInt32BuilderPtr;
typedef void UInt64BuilderPtr;
typedef void Int8BuilderPtr;
typedef void Int16BuilderPtr;
typedef void ChunkedArrayPtr;
typedef void Date32BuilderPtr;
typedef void Date64BuilderPtr;
typedef void Time32BuilderPtr;
typedef void Time64BuilderPtr;
typedef void TimestampBuilderPtr;
typedef void DurationBuilderPtr;
#endif

struct ArrowSchema *arrow_schema(const char*);
struct ArrowSchema *feather_schema(const char*);
struct ArrowSchema *parquet_schema(const char*, int64_t *num_rows);
void free_schema(struct ArrowSchema*);

TablePtr *parquet_read_table(const char *, int *col_idxs, int ncols, int use_threads, int64_t only_first);
TablePtr *feather_read_table(const char *, int *col_idxs, int ncols);
TablePtr *csv_read_table(const char *);
TablePtr *json_read_table(const char *);
TablePtr *table_concatenate(TablePtr **tables, int ntables);
TablePtr *table_slice(TablePtr*, int64_t, int64_t);
int64_t table_num_rows(TablePtr*);
struct ArrowSchema *table_schema(TablePtr*);
void free_table(TablePtr*);

int timestamp_unit_in_ns(TablePtr*, const char*, int);
int time64_unit_in_ns(TablePtr*, const char*, int);
int duration_unit_in_ns(TablePtr*, const char*, int);

struct ArrowArray *table_chunked_column(TablePtr *reader, int column_idx, int *nchunks, int dt);
struct ArrowArray *table_chunked_column_by_name(TablePtr *reader, const char *column_name, int *nchunks, int dt);
void free_chunked_column(struct ArrowArray *, int nchunks);

TablePtr *table_add_all_columns(TablePtr*, TablePtr*);
TablePtr *table_add_column(TablePtr*, const char*, ChunkedArrayPtr*);
ChunkedArrayPtr *table_get_column(TablePtr*, const char*);
void free_chunked_array(ChunkedArrayPtr*);

TablePtr *create_table(struct ArrowArray *array, struct ArrowSchema *schema);
void arrow_write_file(const char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size);
void parquet_write_file(const char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void feather_write_file(const char *filename, struct ArrowArray *, struct ArrowSchema *, int chunk_size, int compression);
void parquet_write_table(const char *filename, TablePtr *table, int chunk_size, int compression);
void feather_write_table(const char *filename, TablePtr *table, int chunk_size, int compression);

ParquetReader *parquet_reader_open(const char *filename, int *col_idxs, int ncols, int use_threads, int mmap, int buffer_size, int batch_size);
TablePtr *parquet_reader_next(ParquetReader *pr);
void parquet_reader_close(ParquetReader *pr);
void parquet_reader_free(ParquetReader *pr);

Int32BuilderPtr *create_int32_builder();
Int64BuilderPtr *create_int64_builder();
Int8BuilderPtr *create_int8_builder();
Int16BuilderPtr *create_int16_builder();
UInt8BuilderPtr *create_uint8_builder();
UInt16BuilderPtr *create_uint16_builder();
UInt32BuilderPtr *create_uint32_builder();
UInt64BuilderPtr *create_uint64_builder();
FloatBuilderPtr *create_float_builder();
DoubleBuilderPtr *create_double_builder();
BooleanBuilderPtr *create_boolean_builder();
StringBuilderPtr *create_string_builder();
Date32BuilderPtr *create_date32_builder();
Date64BuilderPtr *create_date64_builder();
Time32BuilderPtr *create_time32_builder(int unit);
Time64BuilderPtr *create_time64_builder(int unit);
TimestampBuilderPtr *create_timestamp_builder(int unit, const char* timezone);
DurationBuilderPtr *create_duration_builder(int unit);
void append_int32_builder(Int32BuilderPtr*, int32_t);
void append_int64_builder(Int64BuilderPtr*, int64_t);
void append_int8_builder(Int8BuilderPtr*, int8_t);
void append_int16_builder(Int16BuilderPtr*, int16_t);
void append_uint8_builder(UInt8BuilderPtr*, uint8_t);
void append_uint16_builder(UInt16BuilderPtr*, uint16_t);
void append_uint32_builder(UInt32BuilderPtr*, uint32_t);
void append_uint64_builder(UInt64BuilderPtr*, uint64_t);
void append_float_builder(FloatBuilderPtr*, float);
void append_double_builder(DoubleBuilderPtr*, double);
void append_boolean_builder(BooleanBuilderPtr*, int);
void append_string_builder(StringBuilderPtr*, const char*);
void append_date32_builder(Date32BuilderPtr*, int32_t);
void append_date64_builder(Date64BuilderPtr*, int64_t);
void append_time32_builder(Time32BuilderPtr*, int32_t);
void append_time64_builder(Time64BuilderPtr*, int64_t);
void append_timestamp_builder(TimestampBuilderPtr*, int64_t);
void append_duration_builder(DurationBuilderPtr*, int64_t);
void append_null_int32_builder(Int32BuilderPtr*, int);
void append_null_int64_builder(Int64BuilderPtr*, int);
void append_null_int8_builder(Int8BuilderPtr*, int);
void append_null_int16_builder(Int16BuilderPtr*, int);
void append_null_uint8_builder(UInt8BuilderPtr*, int);
void append_null_uint16_builder(UInt16BuilderPtr*, int);
void append_null_uint32_builder(UInt32BuilderPtr*, int);
void append_null_uint64_builder(UInt64BuilderPtr*, int);
void append_null_float_builder(FloatBuilderPtr*, int);
void append_null_double_builder(DoubleBuilderPtr*, int);
void append_null_boolean_builder(BooleanBuilderPtr*, int);
void append_null_string_builder(StringBuilderPtr*, int);
void append_null_date32_builder(Date32BuilderPtr*, int);
void append_null_date64_builder(Date64BuilderPtr*, int);
void append_null_time32_builder(Time32BuilderPtr*, int);
void append_null_time64_builder(Time64BuilderPtr*, int);
void append_null_timestamp_builder(TimestampBuilderPtr*, int);
void append_null_duration_builder(DurationBuilderPtr*, int);
void free_int32_builder(Int32BuilderPtr*);
void free_int64_builder(Int64BuilderPtr*);
void free_int8_builder(Int8BuilderPtr*);
void free_int16_builder(Int16BuilderPtr*);
void free_uint8_builder(UInt8BuilderPtr*);
void free_uint16_builder(UInt16BuilderPtr*);
void free_uint32_builder(UInt32BuilderPtr*);
void free_uint64_builder(UInt64BuilderPtr*);
void free_float_builder(FloatBuilderPtr*);
void free_double_builder(DoubleBuilderPtr*);
void free_boolean_builder(BooleanBuilderPtr*);
void free_string_builder(StringBuilderPtr*);
void free_date32_builder(Date32BuilderPtr*);
void free_date64_builder(Date64BuilderPtr*);
void free_time32_builder(Time32BuilderPtr*);
void free_time64_builder(Time64BuilderPtr*);
void free_timestamp_builder(TimestampBuilderPtr*);
void free_duration_builder(DurationBuilderPtr*);
int64_t length_int32_builder(Int32BuilderPtr*);
int64_t length_int64_builder(Int64BuilderPtr*);
int64_t length_int8_builder(Int8BuilderPtr*);
int64_t length_int16_builder(Int16BuilderPtr*);
int64_t length_uint8_builder(UInt8BuilderPtr*);
int64_t length_uint16_builder(UInt16BuilderPtr*);
int64_t length_uint32_builder(UInt32BuilderPtr*);
int64_t length_uint64_builder(UInt64BuilderPtr*);
int64_t length_float_builder(FloatBuilderPtr*);
int64_t length_double_builder(DoubleBuilderPtr*);
int64_t length_boolean_builder(BooleanBuilderPtr*);
int64_t length_string_builder(StringBuilderPtr*);
int64_t length_date32_builder(Date32BuilderPtr*);
int64_t length_date64_builder(Date64BuilderPtr*);
int64_t length_time32_builder(Time32BuilderPtr*);
int64_t length_time64_builder(Time64BuilderPtr*);
int64_t length_timestamp_builder(TimestampBuilderPtr*);
int64_t length_duration_builder(DurationBuilderPtr*);
int64_t null_count_int32_builder(Int32BuilderPtr*);
int64_t null_count_int64_builder(Int64BuilderPtr*);
int64_t null_count_int8_builder(Int8BuilderPtr*);
int64_t null_count_int16_builder(Int16BuilderPtr*);
int64_t null_count_uint8_builder(UInt8BuilderPtr*);
int64_t null_count_uint16_builder(UInt16BuilderPtr*);
int64_t null_count_uint32_builder(UInt32BuilderPtr*);
int64_t null_count_uint64_builder(UInt64BuilderPtr*);
int64_t null_count_float_builder(FloatBuilderPtr*);
int64_t null_count_double_builder(DoubleBuilderPtr*);
int64_t null_count_boolean_builder(BooleanBuilderPtr*);
int64_t null_count_string_builder(StringBuilderPtr*);
int64_t null_count_date32_builder(Date32BuilderPtr*);
int64_t null_count_date64_builder(Date64BuilderPtr*);
int64_t null_count_time32_builder(Time32BuilderPtr*);
int64_t null_count_time64_builder(Time64BuilderPtr*);
int64_t null_count_timestamp_builder(TimestampBuilderPtr*);
int64_t null_count_duration_builder(DurationBuilderPtr*);
TablePtr *make_table(BuilderPtr**, char**, int);

char *table_to_string(TablePtr*);
#ifdef __cplusplus
}
#endif
#endif
