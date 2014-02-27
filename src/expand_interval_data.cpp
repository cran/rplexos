
#include <Rcpp.h>

using namespace std;

// [[Rcpp::export]]
Rcpp::List expand_interval_data(Rcpp::DataFrame key, Rcpp::DataFrame interval) {
    // Create output
    Rcpp::CharacterVector keyNames = key.attr("names"), intName = interval.attr("names"), outNames;
    Rcpp::List out(keyNames.size() - 2);

    // Fast access to columns in input data
    Rcpp::IntegerVector key_key  = key["key_interval"],
                        key_from = key["time_from"],
                        key_to   = key["time_to"],
                        period   = interval["period"],
                        period2(period.size());
    Rcpp::DatetimeVector date    = interval["time"],
                         date2(date.size());
    
    // Initialize variables
    int numberKeys = 0, numberPeriod = 0, outputSize, prevKey = -1;
    
    // Get number unique keys
    for (int i = 0; i < key_key.size(); ++i) {
        if (prevKey != key_key[i])
            ++numberKeys;
        prevKey = key_key[i];
    }
    
    // Filter periods that are in the data
    for (int j = 0; j < period.size(); ++j) {
        for (int i = 0; i < key_from.size(); ++i) {
            if (period[j] >= key_from[i] && period[j] <= key_to[i]) {
                period2[numberPeriod] = period[j];
                date2[numberPeriod]   = date[j];
                ++numberPeriod;
                
                break;
            }
        }
    }
    
    // Calculate final output size
    outputSize = numberKeys * numberPeriod;
    
    // Create a vector that expands 'key' data frame
    Rcpp::IntegerVector keyExpand(outputSize);
    int currPeriod = 0, currLine = 0, prev_key = -1;
    
    for (int i = 0; i < key_key.size(); ++i) {
        // If we go to a new key, restart the period lookup pointer
        if (key_key[i] != prev_key) {
            currPeriod = 0;
            prev_key = key_key[i];
        }
        
        // If, for some reason, beyond size of period restart the pointer
        if (currPeriod >= numberPeriod)
            currPeriod = 0;
        
        while (currPeriod < numberPeriod) {
            if (period2[currPeriod] > key_to[i])
                break;
            
            keyExpand[currLine] = i;
            ++currPeriod;
            ++currLine;
        }
    }
    
    // Add columns that user requested 
    for (int i = 3; i < keyNames.size() - 1; ++i) {
        Rcpp::CharacterVector newCol(outputSize), dataCol = key(i);
        for (int j = 0; j < outputSize; ++j)
            newCol[j] = dataCol[keyExpand[j]];
        
        out[outNames.size()] = newCol;
        //out[outNames.size()] = keyExpand;
        outNames.push_back(keyNames[i]);
    }
    
    // Add time column
    { Rcpp::DatetimeVector newCol(outputSize);
    int k = 0;
    for (int j = 0; j < outputSize; ++j) {
        if (k == numberPeriod)
            k = 0;
        newCol[j] = date2[k];
        ++k;
    }
    out[outNames.size()] = newCol;
    outNames.push_back(intName[1]); }
    
    // Add value column
    { Rcpp::NumericVector newCol(outputSize), dataCol = key(key.size() - 1);
    for (int j = 0; j < outputSize; ++j)
        newCol[j] = dataCol[keyExpand[j]];
    out[outNames.size()] = newCol;
    outNames.push_back(keyNames[keyNames.size() - 1]); }
    
    // Convert output list to data.frame before returning
    Rcpp::IntegerVector rowNames(2);
    rowNames[0] = NA_INTEGER;
    rowNames[1] = -outputSize;
    out.attr("names") = outNames;
    out.attr("row.names") = rowNames;
    out.attr("class") = "data.frame";
    
    // Output
    return out;
}
