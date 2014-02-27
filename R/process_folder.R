#' Convert PLEXOS solutions to SQLite databases
#'
#' Functions to process PLEXOS solutions and dump them into SQLite databases that are easy
#' to read.
#'
#' \code{process_solution} converts a single zipped PLEXOS solution into a SQLite database.
#' The resulting database file has a simplified structure so data can be retrieved easily.
#' The temporary database contains the original data structure in the PLEXOS solution file.
#'
#' \code{process_folders} is used to process an entire directory. It searches each folder for
#' PLEXOS solutions and applies \code{process_solution} to each one of them.
#'
#' @param folders Folder or folders to process (default is the working folder)
#' @param file Single PLEXOS solution file to process
#' @param keep.temp Should temporary databases be preserved?
#'
#' @examples
#' \dontrun{process_folder()}
#' \dontrun{process_folder("HiWind")}
#' \dontrun{process_solution("HiWind/Model WWSIS_c_RT_CoreB_M01_SC3 Solution.zip")}
#'
#' @export
process_folder <- function(folders = ".", keep.temp = FALSE) {
  # Check inputs
  assert_that(is.character(folders), is.flag(keep.temp))

  # Check that folders exist
  are.dirs <- file.info(folders)$isdir
  are.dirs[is.na(are.dirs)] <- FALSE
  if(!all(are.dirs)) {
    not.dirs <- folders[!are.dirs]
    stop(paste(not.dirs, collapse = ", "), " are not valid folders")
  }
  
  # Iterate through list of folders
  for (folder in folders) {
    message("Folder: '", folder, "'")
    
    # Read list of files in the PLEXOS solution file
    zip.files <- list.files(folder, ".zip$", full.names = TRUE)
    
    # If no ZIP files found, skip to the next folder
    if (length(zip.files) == 0) {
      message("  - No solutions found, skipping folder")
      next
    }
    
    # Process each file
    db.files <- vapply(zip.files, process_solution, "", keep.temp)
    
    message("  - Finished folder!")
  }
  
  TRUE
}
