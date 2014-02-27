
#include <vector>
#include <string>
#include <Rcpp.h>
#include "rapidxml.h"

#define ALT_INDEX "index2"

using namespace std;
using namespace rapidxml;

// Find a string in a vector
int find_position(vector<string> x, string y) {
    int out = -1;
    
    for (unsigned int i = 0; i < x.size(); ++i) {
        if (x.at(i) == y) {
            out = i;
            break;
        }
    }
    
    return(out);
}

// [[Rcpp::export]]
Rcpp::List process_xml(std::string xml) {
    // Start XML variables
    xml_document<> doc;
	xml_node<> *root_node;
    
    // Parse XML file and find root node
    vector<char> contents(xml.size() + 1);
    copy(xml.begin(), xml.end(), contents.begin());
	doc.parse<0> (&contents[0]);
	root_node = doc.first_node();
    
    // Variables for the loop
    vector<string> table_names;
    vector< vector<string> > table_heads;
    vector< vector< vector<string> > > results;
    string prev_table = "", curr_table;
    int prev_pos = -1, pos = -1;
    bool new_table;
    
    // Get list of tables and headers
	for (xml_node<> *param_node = root_node->first_node(); param_node; param_node = param_node->next_sibling()) {
        vector<string> names(0);
        curr_table = string(param_node->name());
        
        // Skip entries to the 'settings' table
        if (curr_table == "settings")
            continue;
        
        // Check if this is a new or existing table
        if (curr_table == prev_table) {
            pos = prev_pos;
            new_table = FALSE;
        } else {
            pos = find_position(table_names, curr_table);
            new_table = (pos < 0);
        }
        
        // Augment results if new table
        if (new_table) {
            vector< vector<string> > blank_table(0);
            results.push_back(blank_table);
            pos = results.size() - 1;
        }
        
        // Interate over the attributes and store data
        for (xml_node<> *attr_node = param_node->first_node(); attr_node; attr_node = attr_node->next_sibling()) {
            string nm = attr_node->name();
            if (nm == "index") {
                nm = ALT_INDEX;
            }
            
            if (new_table){
                vector<string> blank_col(0);
                results[pos].push_back(blank_col);
            }
            
            string value = attr_node->value();
            results[pos][names.size()].push_back(value);
            names.push_back(nm);
        }
        
        // Add name and headers for new table
        if (new_table) {
            table_names.push_back(curr_table);
            table_heads.push_back(names);
        }
        
        // Prepare for next iteration
        prev_table = curr_table;
        prev_pos = pos;
    }
    
    // Create output object
    Rcpp::List out(table_names.size());
    out.attr("names") = table_names;
    
    for (unsigned int i = 0; i < table_names.size(); ++i) {
        // Add new table
        Rcpp::List new_table(table_heads.at(i).size());
        
        // Add columns
        for (unsigned int j = 0; j < table_heads.at(i).size(); ++j) {
            Rcpp::CharacterVector new_col(results.at(i).at(j).size());
            
            for (int k = 0; k < new_col.size(); ++k)
                new_col[k] = results.at(i).at(j).at(k);
            
            new_table[j] = new_col;
        }
        
        // Convert new table to data.frame
        Rcpp::IntegerVector rowNames(2);
        rowNames[0] = NA_INTEGER;
        rowNames[1] = -results.at(i).at(0).size();
        new_table.attr("row.names") = rowNames;
        new_table.attr("names") = table_heads.at(i);
        new_table.attr("class") = "data.frame";
        
        // Add to output
        out[i] = new_table;
    }
    
    // Return output
    return(out);
}
