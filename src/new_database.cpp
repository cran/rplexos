
#include <Rcpp.h>
#include "sqlite3.h"
#include "rapidxml.h"

#define ALT_INDEX "index2"

using namespace std;
using namespace rapidxml;

// Execute a SQLite statement (returns FALSE if successful)
int query(sqlite3 *db, string stmt) {
    char *zErrMsg = 0;
    int rc = sqlite3_exec(db, stmt.c_str(), NULL, NULL, &zErrMsg);
            
    if (rc != SQLITE_OK) {
        Rcpp::Rcout << "SQL error: " << string(zErrMsg) << endl;
        Rcpp::Rcout << "   in query: " << stmt << endl << endl;
        sqlite3_free(zErrMsg);
    }
    
    return (rc != SQLITE_OK);
}

    
// [[Rcpp::export]]
int new_database(std::string db_file, std::string xml) {
    // Start database
    sqlite3 *db;
    sqlite3_open(db_file.c_str(), &db);
    
    // Turn PRAGMA OFF
    query(db, "PRAGMA synchronous = OFF");
    query(db, "PRAGMA journal_mode = MEMORY");
    query(db, "PRAGMA temp_store = MEMORY");
    
    // Create some tables
    query(db, "CREATE TABLE t_data_0 (key_id integer, period_id integer, value double)");
    query(db, "CREATE TABLE t_data_1 (key_id integer, period_id integer, value double)");
    query(db, "CREATE TABLE t_data_2 (key_id integer, period_id integer, value double)");
    query(db, "CREATE TABLE t_data_3 (key_id integer, period_id integer, value double)");
    query(db, "CREATE TABLE t_data_4 (key_id integer, period_id integer, value double)");
    
    query(db, "CREATE TABLE t_phase_1 (interval_id integer, period_id integer)");
    query(db, "CREATE TABLE t_phase_2 (interval_id integer, period_id integer)");
    query(db, "CREATE TABLE t_phase_3 (interval_id integer, period_id integer)");
    query(db, "CREATE TABLE t_phase_4 (interval_id integer, period_id integer)");
    
    query(db, "CREATE TABLE t_key_index (key_id integer, period_type_id integer, position long, length integer, period_offset integer)");
    
    // Start XML variables
    xml_document<> doc;
	xml_node<> *root_node;
    
    // Parse XML file and find root node
    vector<char> contents(xml.size() + 1);
    copy(xml.begin(), xml.end(), contents.begin());
	doc.parse<0> (&contents[0]);
	root_node = doc.first_node();
    
    // Start variable for binding queries
    sqlite3_stmt *bind_stmt = NULL;
    
    // Variables for the loop
    string prev_table = "", curr_table;

    // Begin insert transaction
    query(db, "BEGIN TRANSACTION");
    
	// Iterate over the list of parameters
	for (xml_node<> *param_node = root_node->first_node(); param_node; param_node = param_node->next_sibling()) {
        vector<string> names(0), values(0);
        curr_table = string(param_node->name());
        
        // Skip entries to the 'settings' table
        if (curr_table == "settings")
            continue;
        
        // Interate over the attributes and store data
	    for (xml_node<> *attr_node = param_node->first_node(); attr_node; attr_node = attr_node->next_sibling()) {
            string nm = attr_node->name();
            if (nm == "index") {
                nm = ALT_INDEX;
            }
            names.push_back(nm);
            values.push_back(attr_node->value());
	    }
        
        // If table name changes, prepare binding statement
        if (curr_table != prev_table) {
            if (prev_table != "") {
                sqlite3_finalize(bind_stmt);
                bind_stmt = NULL;
            }
            
            // Prepare text for binding and new table queries
            string cols = "(", cols2 = "(", marks = "(";
            for (unsigned int i = 0; i < names.size(); ++i) {
                string nm = names.at(i);
                
                cols  += nm + ", ";
                marks += "?, ";
                
                if (nm.substr(0, 3) == "is_")
                    cols2 += nm + " bool, ";
                else if (nm.substr(nm.size() - 3, 3) == "_id")
                    cols2 += nm + " int, ";
                else
                    cols2 += nm + " string, ";
            }
            cols = cols.replace(cols.end() - 2, cols.end(), ")");
            cols2 = cols2.replace(cols2.end() - 2, cols2.end(), ")");
            marks = marks.replace(marks.end() - 2, marks.end(), ")");
            
            // Create table if it doesn't exist
            string statement2 = "CREATE TABLE IF NOT EXISTS [" + curr_table + "] " + cols2;
            if (query(db, statement2)) {
                Rcpp::Rcout << "Error creating table" << endl;
            }
            
            // Prepare insert statement
            string statement = "INSERT INTO [" + curr_table + "] " + cols + " values " + marks;
            if (sqlite3_prepare_v2(db, statement.c_str(), -1, &bind_stmt, NULL) != SQLITE_OK) {
                Rcpp::Rcout << "Error in query inserting XML content" << endl;
            }
        }
        
        // Add current values to the query
        for (unsigned int i = 0; i < values.size(); ++i)
            sqlite3_bind_text(bind_stmt, i + 1, values.at(i).c_str(), -1, NULL);
        
        // Submit values
        if (sqlite3_step(bind_stmt) == SQLITE_DONE)
            sqlite3_reset(bind_stmt);
        
        // Finalize entry
        prev_table = curr_table;
    }
    
    // Finalize binding statement
    if (prev_table != "") {
        sqlite3_finalize(bind_stmt);
        bind_stmt = NULL;
    }

    // End insert transaction
    query(db, "END TRANSACTION");
    
    
    // Close database
    sqlite3_close(db);
    
    return 0;
}
