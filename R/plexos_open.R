#' Open all PLEXOS databases
#' 
#' @param folders Character of folder name(s) where the data is located (each folder represents a scenario)
#' @param names Scenario names
#'
#' @seealso \code{\link{plexos_close}}
#' @seealso \code{\link{query_master}}
#' 
#' @export
#' @importFrom plyr ldply
plexos_open <- function(folders = ".", names = folders) {
  # Check inputs
  assert_that(is.character(folders), is.character(folders))
  assert_that(length(folders) == length(names))
  
  # Change default scenario name to something better than '.'
  if ((folders == ".") & (names == "."))
    names <- "default"
  
  # Get database file names
  db.name <- lapply(folders, list.files, pattern = ".db$", full.names = TRUE)
  
  # Check if folders were empty
  if (min(sapply(db.name, length)) == 0L)
    stop("No databases found in the list of folders. Did you forget to use process_folder()?")
  
  for (i in names(db.name)) {
    if (length(db.name[[i]]) == 0) {
      warning("No databases found in folder: ", i)
    }
  }
  
  # Convert into a data.frame
  names(db.name) <- names
  attr(db.name, "split_type") <- "data.frame"
  attr(db.name, "split_labels") <- data.frame(scenario = factor(names, levels = names))
  make_col <- function(x) data.frame(db = x)
  db.name2 <- ldply(db.name, make_col)
  db.name3 <- db.name2 %>% group_by(scenario) %>%
    mutate(position = 1:n())
  
  # Open databases
  out <- lapply(as.character(db.name2$db), src_sqlite)
  names(out) <- db.name2$db
  attr(out, "split_type") <- "data.frame"
  attr(out, "split_labels") <- data.frame(db = db.name2["scenario"])
  
  # Add scenario to each database object
  for (i in 1:nrow(db.name2)) {
    out[[i]]$scenario <- db.name2[i, "scenario"]
    out[[i]]$position <- db.name3[i, "position"]
  }
  
  # Add rplexos class to object
  class(out) <- c("rplexos", class(out))
  
  out
}


#' Close all PLEXOS databases
#'
#' Close all the open connections to PLEXOS SQLite databases. This function
#' completely erases the provided object.
#'
#' @param db PLEXOS database object
#'
#' @seealso \code{\link{plexos_open}} to create the PLEXOS database object
#'
#' @export
#' @importFrom plyr l_ply
plexos_close <- function(db) {
  assert_that(is.rplexos(db))

  # For each database, close the connection
  l_ply(db, function(x) dbDisconnect(x$con))
  
  # Remove object from memory
  rm(list = deparse(substitute(db)), envir = sys.frame(-1))
  
  TRUE
}

# Create custom summary for rplexos objects
#' @export
#' @method summary rplexos
#' @importFrom plyr ldply
#' @importFrom reshape2 dcast
summary.rplexos <- function(object, ...) {
  info <- ldply(object, function(y) data.frame(position = y$position,
                                               scenario = y$scenario,
                                               path     = y$path,
                                               tables   = length(src_tbls(y))))
  print(info, row.names = FALSE)
}

# Create custom visualization for rplexos objects
#' @export
#' @importFrom plyr ldply
#' @importFrom reshape2 dcast
print.rplexos <- function(x, ...) {
  cat("Structure:\n")
  summary(x)
  
  cat("\nTables:\n")
  info <- ldply(x, function(y) data.frame(position = y$position,
                                          table    = src_tbls(y)))
  
  print(dcast(info, table ~ position, fun.aggregate = length, value.var = "table"),
        row.names = FALSE)
}
