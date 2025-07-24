/* Copyright (c) 2023 Renmin University of China
RMDB is licensed under Mulan PSL v2.
You can use this software according to the terms and conditions of the Mulan PSL v2.
You may obtain a copy of Mulan PSL v2 at:
        http://license.coscl.org.cn/MulanPSL2
THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
See the Mulan PSL v2 for more details. */

#include "sm_manager.h"

#include <sys/stat.h>
#include <unistd.h>

#include <fstream>

#include "index/ix.h"
#include "record/rm.h"
#include "record_printer.h"
#include "record/rm_scan.h"

/**
 * @description: Check if it's a directory
 * @return {bool} Return whether it's a directory
 * @param {string&} db_name Database file name, same as directory name
 */
bool SmManager::is_dir(const std::string& db_name) {
    struct stat st;
    return stat(db_name.c_str(), &st) == 0 && S_ISDIR(st.st_mode);
}

/**
 * @description: Create database, all database-related files are stored in the directory with the same name
 * @param {string&} db_name Database name
 */
void SmManager::create_db(const std::string& db_name) {
    if (!is_dir(db_name)) {
        // Create a subdirectory for the database
        std::string cmd = "mkdir " + db_name;
        if (system(cmd.c_str()) < 0) {  // Create a directory named db_name
            throw UnixError();
        }
        if (chdir(db_name.c_str()) < 0) {  // Enter the directory named db_name
            throw UnixError();
        }
        // Create system directory
        DbMeta *new_db = new DbMeta();
        new_db->name_ = db_name;

        // Note: ofstream creates and opens a file named DB_META_NAME in the current directory
        std::ofstream ofs(DB_META_NAME);

        // Write information from new_db to the DB_META_NAME file using the defined operator<<
        ofs << *new_db;  // Note: operator<< is overloaded here

        delete new_db;

        // Create log file
        disk_manager_->create_file(LOG_FILE_NAME);

        // Return to root directory
        if (chdir("..") < 0) {
            throw UnixError();
        }
    } else {
        throw DatabaseExistsError(db_name);
    }
}

/**
 * @description: Delete database and clear related files and database directory
 * @param {string&} db_name Database name, same as directory name
 */
void SmManager::drop_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        std::string cmd = "rm -r " + db_name;
        if (system(cmd.c_str()) < 0) {
            throw UnixError();
        }
    } else {
        throw DatabaseNotFoundError(db_name);
    }
}

/**
 * @description: Open database, find corresponding directory and load database metadata and related files
 * @param {string&} db_name Database name, same as directory name
 */
void SmManager::open_db(const std::string& db_name) {
    if (is_dir(db_name)) {
        if (db_.name_.empty()) {
            // Set working directory to ensure output.txt and table files are stored in database directory
            if (chdir(db_name.c_str()) < 0) {
                throw InternalError("Failed to change directory to " + db_name);
            }

            // Load database metadata
            std::ifstream ifs(DB_META_NAME);
            if (!ifs.fail()) {
                ifs >> db_;  // Note: operator>> is overloaded here, reading DB_META_NAME file content into db_

                // Load table metadata
                for (auto &t : db_.tabs_) {
                    auto &tab = t.second;
                    // Load table file to fhs_
                    fhs_.emplace(tab.name, rm_manager_->open_file(tab.name));

                    // Load index files to ihs_
                    for (auto &index : tab.indexes) {
                        const std::string &index_name = index.first;
                        ihs_.emplace(index_name, ix_manager_->open_index(index_name));
                    }
                }
            } else {
                throw InternalError("Failed to open database metadata file: " + DB_META_NAME);
            }
        } else {
            throw DatabaseExistsError(db_name);
        }
    } else {
        throw DatabaseNotFoundError(db_name);
    }
}

/**
 * @description: Flush database-related metadata to disk
 */
void SmManager::flush_meta() {
    // Clear file by default
    std::ofstream ofs(DB_META_NAME);
    ofs << db_;
}

/**
 * @description: Close database and persist data
 */
void SmManager::close_db() {
    // Flush all dirty pages
    for (auto &rm_file : fhs_) {
        buffer_pool_manager_->flush_all_pages(rm_file.second->GetFd());
    }
    // Refresh metadata
    flush_meta();
    db_.name_.clear();
    db_.tabs_.clear();

    // Close all table files
    for (auto &tab_file : fhs_) {
        rm_manager_->close_file(tab_file.second.get());
    }
    fhs_.clear();

    // Close all index files
    for (auto &index_file : ihs_) {
        ix_manager_->close_index(index_file.second.get());
    }
    ihs_.clear();

    // Return to root directory
    if (chdir("..") < 0) {
        throw UnixError();
    }
}

/**
 * @description: Show all tables, write results to output.txt for testing
 * @param {Context*} context 
 */
void SmManager::show_tables(Context* context) {
    std::fstream outfile;
    outfile.open("output.txt", std::ios::out | std::ios::app);
    outfile << "| Tables |\n";
    RecordPrinter printer(1);
    printer.print_separator(context);
    printer.print_record({"Tables"}, context);
    printer.print_separator(context);
    for (auto &entry : db_.tabs_) {
        auto &tab = entry.second;
        printer.print_record({tab.name}, context);
        outfile << "| " << tab.name << " |\n";
    }
    printer.print_separator(context);
    outfile.close();
}

/**
 * @description: Show table metadata
 * @param {string&} tab_name Table name
 * @param {Context*} context 
 */
void SmManager::desc_table(const std::string& tab_name, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);

    std::vector<std::string> captions = {"Field", "Type", "Index"};
    RecordPrinter printer(captions.size());
    // Print header
    printer.print_separator(context);
    printer.print_record(captions, context);
    printer.print_separator(context);
    // Print fields
    for (auto &col : tab.cols) {
        std::vector<std::string> field_info = {col.name, coltype2str(col.type), col.index ? "YES" : "NO"};
        printer.print_record(field_info, context);
    }
    // Print footer
    printer.print_separator(context);
}

/**
 * @description: Create table
 * @param {string&} tab_name Table name
 * @param {vector<ColDef>&} col_defs Table fields
 * @param {Context*} context 
 */
void SmManager::create_table(const std::string& tab_name, const std::vector<ColDef>& col_defs, Context* context) {
    if (!db_.is_table(tab_name)) {
        // Create table meta
        int curr_offset = 0;
        TabMeta tab;
        tab.name = tab_name;
        for (auto &col_def : col_defs) {
            ColMeta col = {.tab_name = tab_name,
                           .name = col_def.name,
                           .type = col_def.type,
                           .len = col_def.len,
                           .offset = curr_offset,
                           .index = false};
            curr_offset += col_def.len;
            tab.cols.push_back(col);
        }
        // Create & open record file
        int record_size = curr_offset;  // record_size is the size occupied by col meta
        rm_manager_->create_file(tab_name, record_size);
        db_.tabs_[tab_name] = tab;
        fhs_.emplace(tab_name, rm_manager_->open_file(tab_name));

        flush_meta();
    } else {
        throw TableExistsError(tab_name);
    }
}

/**
 * @description: Drop table
 * @param {string&} tab_name Table name
 * @param {Context*} context
 */
void SmManager::drop_table(const std::string& tab_name, Context* context) {
    if (db_.is_table(tab_name)) {
        // Delete table file
        rm_manager_->close_file(fhs_[tab_name].get());
        rm_manager_->destroy_file(tab_name);
        fhs_.erase(tab_name);  // Remove corresponding file handle
        db_.tabs_.erase(tab_name);

        // Update metadata
        flush_meta();
    } else {
        throw TableNotFoundError(tab_name);
    }
}

/**
 * @description: Create index
 * @param {string&} tab_name Table name
 * @param {vector<string>&} col_names Column names included in index
 * @param {Context*} context
 */
void SmManager::create_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    // Throw exception if index file doesn't exist
    if (!disk_manager_->is_file(index_name)) {
        TabMeta &tab = db_.get_table(tab_name);
        // Check if all fields exist
        bool all_cols_exist = true;
        for (const auto &col_name : col_names) {
            if (!tab.is_col(col_name)) {
                all_cols_exist = false;
                throw ColumnNotFoundError(col_name);
            }
        }

        if (all_cols_exist) {
            // Fields exist, create index normally, first get field metadata
            std::vector<ColMeta> cols;
            cols.reserve(col_names.size());
            int len = 0;
            for (const auto &col_name : col_names) {
                auto col_meta = tab.get_col(col_name);
                cols.push_back(*col_meta);
                len += col_meta->len;
            }

            // Create index metadata
            ix_manager_->create_index(index_name, cols);
            std::unique_ptr<IxIndexHandle> ix_handle = ix_manager_->open_index(index_name);
            RmFileHandle* file_handle = fhs_[tab_name].get();

            // Index is ordered storage structure for records, using B+ tree
            // Unique index prevents duplicate insertion
            // Allocate index key space
            char* key = new char[len];
            // Use previously implemented RmScan to scan table records
            int pos = 0;
            auto scan = std::make_unique<RmScan>(file_handle);
            while (!scan->is_end()) {
                auto rid = scan->rid();
                auto record = file_handle->get_record(rid, context);
                pos = 0;
                for (const auto &col_meta: cols) {
                    memcpy(key + pos, record->data + col_meta.offset, col_meta.len);
                    pos += col_meta.len;
                }

                // Insert B+ tree index
                page_id_t result = ix_handle->insert_entry(key, rid, context->txn_);
                if (result == IX_NO_PAGE) {
                    delete[] key;  // Free key memory
                    ix_manager_->close_index(ix_handle.get());
                    ix_manager_->destroy_index(index_name);
                    throw InternalError("Duplicate key found when creating unique index: " + index_name);
                }
                scan->next();
            }

            delete [] key;  // Free key memory
            // Update table index information
            tab.indexes[index_name] = IndexMeta {
                tab_name,
                len,
                static_cast<int>(col_names.size()),
                cols
            };

            ihs_.emplace(index_name, std::move(ix_handle));  
            // Store index handle in ihs_ for unified management
            flush_meta();
        }
    } else {
        throw IndexExistsError(tab_name, col_names);
    }
}

/**
 * @description: Show all indexes for the table, write results to output.txt for testing
 * @param {String} table_name {Context*} context
 */
// Helper function: format index column names
std::string format_index_cols(const std::vector<ColMeta>& cols) {
    std::string result = "(";
    for (size_t i = 0; i < cols.size(); ++i) {
        if (i > 0) result += ",";
        result += cols[i].name;
    }
    result += ")";
    return result;
}

void SmManager::show_indexes(const std::string &table_name, Context *context) {
    std::ofstream outfile("output.txt", std::ios::out | std::ios::app);
    RecordPrinter printer(3);
    std::vector<std::string> rec_str{table_name, "unique", ""};

    for (const auto &[_, index]: db_.tabs_[table_name].indexes) {
        std::string cols_str = format_index_cols(index.cols);
        // File output
        outfile << "| " << table_name << " | unique | " << cols_str << " |\n";
        // Buffer output
        rec_str[2] = cols_str;
        printer.print_indexes(rec_str, context);
    }
}

/**
 * @description: Drop index
 * @param {string&} tab_name Table name
 * @param {vector<string>&} col_names Column names included in index
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<std::string>& col_names, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    // Get table metadata
    std::string index_name = ix_manager_->get_index_name(tab_name, col_names);
    // Can delete existing index file
    if (disk_manager_->is_file(index_name)) {
        ix_manager_->close_index(ihs_[index_name].get());
        ix_manager_->destroy_index(index_name);
        ihs_.erase(index_name);  
        // Remove index file handle from ihs_
        tab.indexes.erase(index_name);  
        // Remove index info from table metadata
        flush_meta();
    } else {
        throw IndexNotFoundError(tab_name, col_names);
    }
}

/**
 * @description: Drop index
 * @param {string&} tab_name Table name
 * @param {vector<ColMeta>&} cols Column metadata included in index
 * @param {Context*} context
 */
void SmManager::drop_index(const std::string& tab_name, const std::vector<ColMeta>& cols, Context* context) {
    TabMeta &tab = db_.get_table(tab_name);
    // Get table metadata
    std::string index_name = ix_manager_->get_index_name(tab_name, cols);
    // Can delete existing index file
    if (disk_manager_->is_file(index_name)) {
        ix_manager_->close_index(ihs_[index_name].get());
        ix_manager_->destroy_index(index_name);
        ihs_.erase(index_name);
        // Remove index file handle from ihs_
        tab.indexes.erase(index_name);  
        // Remove index info from table metadata
        flush_meta();
    } else {
        std::vector<std::string> col_names;
        col_names.reserve(cols.size());
        for (const auto &col : cols) {
            col_names.push_back(col.name);
        }
        throw IndexNotFoundError(tab_name, col_names);
    }
}

// Statistics-related method implementations
size_t SmManager::getTableRowCount(const std::string& tab_name) {
    if (db_.is_table(tab_name)) {
        auto it = fhs_.find(tab_name);
        if (it != fhs_.end()) {
            RmFileHandle* fh = it->second.get();
            size_t row_count = 0;
            
            // Scan entire table to count rows
            try {
                auto scan = std::make_unique<RmScan>(fh);
                while (!scan->is_end()) {
                    row_count++;
                    scan->next();
                }
            } catch (...) {
                // Return 0 if scan fails
                return 0;
            }
            
            return row_count;
        } else {
            return 0;
        }
    } else {
        throw TableNotFoundError(tab_name);
    }
}

std::vector<size_t> SmManager::getColumnCardinalities(const std::string& tab_name) {
    std::vector<size_t> cardinalities;
    
    if (db_.is_table(tab_name)) {
        TabMeta& tab = db_.get_table(tab_name);
        auto it = fhs_.find(tab_name);
        if (it != fhs_.end()) {
            // Initialize cardinalities to 0 for each column
            cardinalities.resize(tab.cols.size(), 0);
            
            // In actual implementation, more precise statistics should be maintained
            // Here simplified to return estimated values based on table row count
            size_t row_count = getTableRowCount(tab_name);
            for (size_t i = 0; i < cardinalities.size(); i++) {
                // Assume each column's cardinality is about 70% of row count (estimated)
                cardinalities[i] = static_cast<size_t>(row_count * 0.7);
            }
        } else {
            // Return empty cardinality info if table file doesn't exist
            cardinalities.resize(tab.cols.size(), 0);
        }
    } else {
        throw TableNotFoundError(tab_name);
    }
    
    return cardinalities;
}

double SmManager::getSelectivity(const std::string& tab_name, const std::string& col_name, CompOp op) {
    if (db_.is_table(tab_name)) {
        TabMeta& tab = db_.get_table(tab_name);
        if (tab.is_col(col_name)) {
            // Simplified selectivity estimation
            switch (op) {
                case OP_EQ:
                    return 0.1;   // Equality condition selectivity is 10%
                case OP_NE:
                    return 0.9;   // Not equal condition selectivity is 90%
                case OP_LT:
                case OP_LE:
                case OP_GT:
                case OP_GE:
                    return 0.33;  // Range condition selectivity is 33%
                default:
                    return 0.5;   // Default selectivity is 50%
            }
        } else {
            throw ColumnNotFoundError(col_name);
        }
    } else {
        throw TableNotFoundError(tab_name);
    }
}