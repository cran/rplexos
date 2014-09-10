# Clean spaces and special characters from strings
clean_string <- function(x) {
  gsub(" |&|'|-", "", x)
}

# Same as clean_string, but makes sure that column exists
safe_clean_string <- function(x, column) {
  if (column %in% names(x)) {
    out <- clean_string(x[, column])
  } else {
    out <- rep("", nrow(x))
  }
  
  out
}

# Delete file and give error if unsuccesfull
stop_ifnot_delete <- function(x) {
  # Error if file cannot be removed
  if (!file.remove(x))
    stop("Unable to delete file: ", x, call. = FALSE)
}

# Regroup with characters
regroup_char <- function(x, vars, ...) {
  vars2 <- lapply(vars, as.symbol)
  regroup(x, vars2, ...)
}


#' Get list of valid columns
#'
#' List of valid columns accepted in \code{\link{query_master}}, \code{\link{sum_master}} and related functions.
#'
#' @seealso \code{\link{query_master}}, \code{\link{sum_master}}
#'
#' @export
valid_columns <- function() c("collection", "property", "name", "parent", "category", "region", "zone",
                              "period_type_id", "band_id", "sample_id", "timeslice_id", "time")

#' Get list of folders in the working directory
#'
#' List of existing folders in the working directory. This function is used when the wildcard symbol (\code{"*"})
#' is provided to the \code{\link{process_folder}} and \code{\link{plexos_open}} functions.
#'
#' @seealso \code{\link{setwd}}, \code{\link{process_folder}}, \code{\link{plexos_open}}
#'
#' @export
list_folders <- function() {
  f <- dir()
  f[file.info(f)$isdir]
}


# *** assert_that validation functions ***

# Check that columns are valid
are_columns <- function(col) all(col %in% valid_columns())

on_failure(are_columns) <- function(call, env) {
  paste0("Incorrect column parameter. Use valid_columns() to get the full list.")
}

# Check that names are valid columns
names_are_columns <- function(x) are_columns(names(x))

on_failure(names_are_columns) <- function(call, env) {
  paste0("The names in ", deparse(call$x), " must correspond to correct columns. Use valid_columns() to get the full list.")
}

# Check that names are valid columns
time_not_a_name <- function(x) !"time" %in% (names(x))

on_failure(time_not_a_name) <- function(call, env) {
  paste0("time should not be an entry in ", deparse(call$x), ". Use time.range instead.")
}

# Check that object is valid
is.rplexos <- function(x) inherits(x, "rplexos")

on_failure(is.rplexos) <- function(call, env) {
  paste0(eval(call$x, env), " is not a valid database object. 'db' should be created with plexos_open().")
}

# Check date range inputs
correct_date <- function(x) all(!is.na(x))

on_failure(correct_date) <- function(call, env) {
  paste0("Could not convert time.range. Use 'ymdhms' or 'ymd' formats")
}

# Check phase inputs
correct_phase <- function(x) x %in% 1:4

on_failure(correct_phase) <- function(call, env) {
  paste0("'phase' must be one of: 1 (LT), 2 (PASA), 3 (MT) or 4 (ST)")
}

# Check that a vector of characters are folder names
is_folder <- function(x) {
  if (length(x) == 1) {
    if(x == "*") {
      return(TRUE)
    }
  }
  all(file.exists(x)) & all(file.info(x)$isdir, na.rm = FALSE)
}

on_failure(is_folder) <- function(call, env) {
  paste0("'folders' must be a vector of existing folders or the wildcard \"*\"")
}
