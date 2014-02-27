# Clean spaces and special characters from strings
clean_string <- function(x) {
  gsub(" |&|'|-", "", x)
}

# Regroup with characters
regroup_char <- function(x, vars, ...) {
  vars2 <- lapply(vars, as.symbol)
  regroup(x, vars2, ...)
}

# Fast version of llply that adds scenario
#' @importFrom plyr llply
fast_ldply <- function(db, ..., filter.repeats = FALSE) {
  out <- llply(db, ..., .progress = "none")
  for (i in names(out)) {
    if (nrow(out[[i]])) {
      out[[i]]$scenario <- db[[i]]$scenario
      if (filter.repeats)
        out[[i]]$position <- db[[i]]$position
    }
  }
  out2 <- rbind_all(out)
  
  if (filter.repeats) {
    # Get option to see how to deal with ties (defaults to last)
    tieopt <- getOption("rplexos.tiebreak")
    if (is.null(tieopt)) {
      tieopt <- "last"
    } else if (!tieopt %in% c("first", "last", "all")) {
      warning("Invalid 'rplexos.tiebreak' option (must be one of: first, last, all). Using last instead")
      tieopt <- "last"
    }
    
    if (tieopt %in% c("first", "last")) {
      # Group by time
      out2 <- out2 %>%
        group_by(time)
    
      if (tieopt == "last") {
        # If there are repeats, use the latter entry
        out2 <- out2 %>%
          filter(position == max(position))
      } else {
        # If there are repeats, use the latter entry
        out2 <- out2 %>%
          filter(position == min(position))
      }
      
      # Ungroup and delete path column
      out2 <- out2 %>%
        ungroup() %>%
        select(-position)
    }
  }
  
  out2
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


# *** assert_that validation functions ***

# Check that property is valid
property_exists <- function(d, table, col, prop) {
  is.summ <- ifelse(table == "interval", 0, 1)
  q <- sprintf("SELECT * from property
               WHERE collection = '%s' AND property = '%s' AND is_summary = %s",
               col, prop, is.summ)
  res <- query_scenario(d, q)
  nrow(res) == length(d)
}

on_failure(property_exists) <- function(call, env) {
  paste0("Property '", eval(call$prop, env), "' in collection '", eval(call$col, env),
         "' is not valid. Use query_property() for list of properties.")
}

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
  paste0("Invalid database object provided. 'db' should be created with plexos_open().")
}

# Check date range inputs
correct_date <- function(x) all(!is.na(x))

on_failure(correct_date) <- function(call, env) {
  paste0("Could not convert time.range. Use 'ymdhms' or 'ymd' formats")
}

# Check phase inputs
correct_phase <- function(x) x %in% 1:4

on_failure(correct_phase) <- function(call, env) {
  paste0("Phase must be one of: 1 (LT), 2 (PASA), 3 (MT) or 4 (ST)")
}
