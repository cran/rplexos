
# Get query for all scenarios
query_scenario <- function(db, query) {
  # Check inputs
  assert_that(is.rplexos(db), is.string(query))
  
  fast_ldply(db, function(x) dbGetQuery(x$con, query))
}


#' Get list of available properties
#'
#' Produce a list of available properties, their units and the collections they belong to.
#' Additionally, a column is created for each scenario that indicates in how many databases
#' the property appears.
#'
#' @param db PLEXOS database object
#'
#' @seealso \code{\link{plexos_open}} to create the PLEXOS database object
#'
#' @export
#' @importFrom reshape2 dcast
query_property <- function(db) {
  out <- query_scenario(db, "SELECT * from property")
  phases <- c("LT", "PASA", "MT", "ST")
  phases.df <- data.frame(phase_id = 1:4, phase = factor(phases, levels = phases))
  out2 <- out %>% inner_join(phases.df, by = "phase_id")
  dcast(out2, phase_id + phase + is_summary + collection + property + unit ~ scenario,
        length, value.var = "unit")
}


# Query databases ***********************************************************************

#' Query data and aggregate data
#' 
#' This collection of functions retrieves data from the processed PLEXOS solutions and
#' returns it in a convenient format.
#' 
#' The family \code{query_*} returns the raw data in the databases, while \code{sum_*}
#' aggregates the data according to \code{columns}.
#'
#' The functions \code{*_day}, \code{*_week}, \code{*_month} and \code{*_year} are
#' shortcuts for the corresponding, \code{*_master} function.
#' 
#' The following is a list of valid items for \code{columns} and filtering. Additionally,
#' \code{time} can be specified for summary data (interval data always includes \code{time}).
#' \itemize{
#'   \item{\code{category}}
#'   \item{\code{property}}
#'   \item{\code{name} (default for columns)}
#'   \item{\code{parent}}
#'   \item{\code{category}}
#'   \item{\code{region} (only meaningful for generators)}
#'   \item{\code{zone} (only meaningful for generators)}
#'   \item{\code{period_type_id}}
#'   \item{\code{band_id}}
#'   \item{\code{sample_id}}
#'   \item{\code{timeslice_id}}
#' }
#' 
#' If defined, the \code{filter} parameter must be a \code{list}. The elements must be chracter
#' vectors and need to have a valid column name (see previous bullet points). For example, one
#' could define it as follows:
#' 
#' \code{filter = list(name = c("Generator1", "Generator2"), region = "Region1")}
#' 
#' To filter by time use the \code{time.range} parameter, instead of adding it as an entry in the
#' \code{filter} parameter.
#' 
#' If a scenario has multiple databases, the data will be aggregated automatically. If two or more
#' databases within the same scenario have overlapping time periods, the default is to select the
#' data from the last database (execute \code{summary(db)} so see the order). To change this behavior
#' set the global option \code{rplexos.tiebreak} to \code{first}, \code{last}, or \code{all} to
#' select data from the first database, the last one or keep all of them.
#' 
#' @param db PLEXOS database object
#' @param time character. Table to query from (interval, day, week, month, year)
#' @param col character. Collection to query
#' @param prop character. Property to query
#' @param columns character. Data columns to query or aggregate by (defaults to \code{name})
#' @param time.range character. Range of dates (Give in 'ymdhms' or 'ymd' format)
#' @param filter list. Used to filter by data columns (see details)
#' @param phase integer. PLEXOS optimization phase (1-LT, 2-PASA, 3-MT, 4-ST)
#' @param ... parameters passed from shortcut functions to master (all except \code{time})
#' 
#' @return A data frame that contains data summarized/aggregated by scenario.
#' 
#' @seealso \code{\link{plexos_open}} to create the PLEXOS database object
#' 
#' @export
#' @importFrom lubridate parse_date_time
query_master <- function(db, time, col, prop, columns = "name", time.range = NULL, filter = NULL, phase = 4) {
  # Check inputs
  assert_that(is.rplexos(db))
  assert_that(is.string(time), is.string(col), is.string(prop), is.character(columns), is.scalar(phase))
  assert_that(correct_phase(phase))
  assert_that(property_exists(db, time, col, prop))
  assert_that(are_columns(columns))
  
  # Time range checks and convert to POSIXct
  if (!is.null(time.range)) {
    assert_that(is.character(time.range), length(time.range) == 2)
    time.range <- parse_date_time(time.range, c("ymdhms", "ymd"), quiet = TRUE)
    assert_that(correct_date(time.range))
  }
  
  # Key filter checks
  if (!is.null(filter)) {
    assert_that(is.list(filter))
    assert_that(names_are_columns(filter))
    assert_that(time_not_a_name(filter))
  }
  
  # Query data
  fast_ldply(db, query_master_each, time, col, prop, columns,
             time.range, filter, phase, filter.repeats = TRUE)
}

#' @rdname query_master
#' @export
query_interval <- function(db, ...) query_master(db, "interval", ...)
#' @rdname query_master
#' @export
query_day      <- function(db, ...) query_master(db, "day", ...)
#' @rdname query_master
#' @export
query_week     <- function(db, ...) query_master(db, "week", ...)
#' @rdname query_master
#' @export
query_month    <- function(db, ...) query_master(db, "month", ...)
#' @rdname query_master
#' @export
query_year     <- function(db, ...) query_master(db, "year", ...)


# Query interval for each database
#' @importFrom lubridate ymd_hms
query_master_each <- function(db, time, col, prop, columns, time.range, filter, phase) {
  # Summary data
  if (time != "interval") {
    # Always query time column
    columns <- c(setdiff(columns, "time"), "time")
    
    col.codes <- paste(columns, collapse = ", ")
    q <- sprintf("SELECT %s, value from %s
                  WHERE collection = '%s' AND property = '%s' AND phase_id = %s",
                  col.codes, time, col, prop, phase)
    
    # Add time filter condition
    if (!is.null(time.range))
      q <- paste0(q, filter_time_query(time, time.range))
    
    # Add key filter condition
    if (!is.null(filter))
      q <- paste0(q, filter_key_query(time, filter))
    
    # Query and format
    out <- dbGetQuery(db$con, q)
    if (nrow(out) > 0) {
      if ("time" %in% names(out))
        out$time <- ymd_hms(out$time)
      for (i in setdiff(columns, "time"))
        out[[i]] <- factor(out[[i]])
    }
    return(out)
  }

  # Interval data, Get time data
  q <- sprintf("SELECT period, datetime(time) AS time FROM time
                WHERE phase_id = %s", phase)
  
  # Add time filter condition
  if (!is.null(time.range))
      q <- paste0(q, filter_time_query(time, time.range))
  
  q <- paste0(q, " ORDER BY period")
  time.data <- dbGetQuery(db$con, q)
  
  # If time data is empty, return an empty data frame
  if (nrow(time.data) == 0)
    return(data.frame())
  
  # Convert into R's time data format
  time.data$time <- ymd_hms(time.data$time)
  
  # Get interval data
  table <- paste("data", "interval", col, prop, sep = "_")
  columns2 <- setdiff(columns, "time")
  columns2 <- paste0(", k.", columns2, collapse = "")
  
  q <- sprintf("SELECT d.key_interval, d.time_from, d.time_to %s, d.value
                FROM %s d NATURAL JOIN key_interval k
                WHERE d.time_from <= %s AND d.time_to >= %s AND phase_id = %s",
                columns2, table, max(time.data$period), min(time.data$period), phase)
  int.data <- dbGetQuery(db$con, q)
  
  # Add key filter condition
  if (!is.null(filter))
    q <- paste0(q, filter_key_query(time, filter))
  
  # If interval data is empty, return an empty data frame
  if (nrow(int.data) == 0)
    return(data.frame())
  
  # Expand time series (in C++) and restore timezone
  out <- expand_interval_data(int.data, time.data)
  attributes(out$time) <- attributes(time.data$time)
  
  # Convert columns to factors
  for (i in setdiff(columns, "time"))
    out[[i]] <- factor(out[[i]])
  
  out
}


# Aggregation ***************************************************************************

#' @rdname query_master
#' @export
#' @importFrom lubridate parse_date_time
sum_master <- function(db, time, col, prop, columns = "name", time.range = NULL, filter = NULL, phase = 4) {
  # Check inputs
  assert_that(is.rplexos(db))
  assert_that(is.string(time), is.string(col), is.string(prop), is.character(columns), is.scalar(phase))
  assert_that(correct_phase(phase))
  assert_that(property_exists(db, time, col, prop))
  assert_that(are_columns(columns))
  
  # Time range checks and convert to POSIXct
  if (!is.null(time.range)) {
    assert_that(is.character(time.range), length(time.range) == 2)
    time.range <- parse_date_time(time.range, c("ymdhms", "ymd"), quiet = TRUE)
    assert_that(correct_date(time.range))
  }
  
  # Key filter checks
  if (!is.null(filter)) {
    assert_that(is.list(filter))
    assert_that(names_are_columns(filter))
    assert_that(time_not_a_name(filter))
  }
  
  # Make sure to include time
  columns2 <- c(setdiff(columns, "time"), "time")
  
  # Query data
  df <- fast_ldply(db, sum_master_each, time, col, prop, columns2,
                   time.range, filter, phase, filter.repeats = TRUE)
  
  # If empty query is returned, return empty data.frame
  if(nrow(df) == 0)
    return(df)
  
  # Aggregate the data
  df %>% regroup_char(c("scenario", columns)) %>%
    summarise(value = sum(value))
}

#' @rdname query_master
#' @export
sum_interval <- function(db, ...) sum_master(db, "interval", ...)
#' @rdname query_master
#' @export
sum_day      <- function(db, ...) sum_master(db, "day", ...)
#' @rdname query_master
#' @export
sum_week     <- function(db, ...) sum_master(db, "week", ...)
#' @rdname query_master
#' @export
sum_month    <- function(db, ...) sum_master(db, "month", ...)
#' @rdname query_master
#' @export
sum_year     <- function(db, ...) sum_master(db, "year", ...)

# Process each database
sum_master_each <- function(db, time, col, prop, columns, time.range, filter, phase) {
  # Query data
  df <- query_master_each(db, time, col, prop, columns, time.range, filter, phase)
  
  # If empty query is returned, return empty data.frame
  if(nrow(df) == 0)
    return(df)
  
  # Aggregate the data
  df %>% regroup_char(columns) %>%
    summarise(value = sum(value))
}


# Filtering *****************************************************************************

# Filter by date
filter_time_query <- function(table, r) {
  var <- ifelse(table == "interval", "datetime(time)", "time")
  sprintf(" AND %s BETWEEN '%s' AND '%s'", var, r[1], r[2])
}

# Other filters
filter_key_query <- function(table, x) {
  if (table == "interval")
    names(x) <- paste0("k.", names(x))
  
  out <- character(0)
  
  for (i in 1:length(x)) {
    if (length(x[[i]]) == 1) {
      q <- sprintf("AND %s = '%s'", names(x)[i], x[[i]])
      out <- paste(out, q)
    } else if (length(x[[i]]) > 1) {
      q1 <- paste("'", x[[i]], "'", sep = "", collapse = ", ")
      q <- sprintf("AND %s IN (%s)", names(x)[i], q1)
      out <- paste(out, q)
    }
  }
  
  out
}
