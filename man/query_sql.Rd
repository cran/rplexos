% Generated by roxygen2: do not edit by hand
% Please edit documentation in R/query.R
\name{query_sql}
\alias{query_sql}
\title{Get query for all scenarios}
\usage{
query_sql(db, sql)
}
\arguments{
\item{db}{PLEXOS database object}

\item{sql}{String containing the SQL query to be performed}
}
\description{
Send a SQL query to all the files in a PLEXOS database object.
}
\examples{
# Process the folder with the solution file provided by rplexos
location <- location_solution_rplexos()
process_folder(location)

# Query data
db <- plexos_open(location)
query_sql(db, "SELECT * from day")
query_sql(db, "SELECT * from time")

}
\seealso{
\code{\link{plexos_open}} to create the PLEXOS database object

\code{\link{query_master}} to perform standard queries of data

Other special.queries: \code{\link{query_class}},
  \code{\link{query_config}}, \code{\link{query_log}},
  \code{\link{query_phase}}, \code{\link{query_property}},
  \code{\link{query_time}}
}

