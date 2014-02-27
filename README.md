# `rplexos`

An R package to read and analyze PLEXOS solutions.

It will eventually merge the standalone parser and the old analysis code from WWSIS-2.


# Installation

`rplexos` requires that you have installed (at least) version 3.1 of R.

## Windows

Copy the [latest release](https://github.nrel.gov/eibanez/rplexos/releases) file (e.g., `rplexos_0.4.zip`) to your working directory and install it with all 
the dependencies

```
install.packages("rplexos_0.2.4.zip")
install.packages("Rcpp")
install.packages("dplyr")
install.packages("plyr")
install.packages("reshape2")
install.packages("lubridate")
install.packages("assertthat")
install.packages("DBI")
install.packages("RSQLite")
install.packages("RSQLite.extfuns")
```


## Mac and Linux

I haven't tested this, but the following should get you close. First, install the dependencies

```
install.packages("Rcpp")
install.packages("dplyr")
install.packages("plyr")
install.packages("reshape2")
install.packages("lubridate")
install.packages("assertthat")
install.packages("DBI")
install.packages("RSQLite")
install.packages("RSQLite.extfuns")
install.packages("devtools")
```

Then install the package from the git repository
```
devtools::install_git("https://github.nrel.gov/eibanez/rplexos.git")
```



# Processing solutions

The following is the typical workflow:


## Organize solutions


Then create a folder for each of your scenarios. If a run is divided in different solution files, copy them
together in the same folder (a DA run a RT run are considered to be different).

For each folder, run the following command, where `folder` is a string (or a vector of strings)

```
process_folder(folder)
```

For example, let us assume we have two scenarios loaded in two folders: `HiWind` and `HiSolar`. 


## Load package

Once in R, load the package into memory

```
library(rplexos)
```


## Process folders

Each ZIP file needs to be converted into a SQLite database. We do so with `process_folder`. This only needs
to be done once.

```
process_folder(c("HiWind", "HiSolar")}
```


## Open databases

Once the databases have been processed, we can access them by first opening a connection with `plexos_open`.
Scenarios names can be defined at this point (if not provided, they default to the folder names).

```
db <- plexos_open(c("HiWind", "HiSolar"), name = c("High Wind", "High Solar")
```


## Query data

We can now use the `db` object to query and aggregate data using thethe functions documented in `query_master`.

```
result1 <- query_interval(a, "Generators", "NetGeneration")
result2 <- sum_interval(a, "Generators", "CapacityCurtailed", "zone", c("2020-07-01", "2020-07-08"))
result3 <- query_month(a, "Generators", "Generation")
result4 <- query_month(a, "Generators", "UnitsStarted", c("category", "region"))
```

The list of available properties can be seen with `query_property`.


## Close databases

Optionally, you can break the connections with `plexos_close`. Closing R has the same effect.
