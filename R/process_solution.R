#' @rdname process_folder
#' 
#' @useDynLib rplexos
#' @export
process_solution <- function(file, keep.temp = FALSE) {
  # Check that inputs are valid
  assert_that(is.string(file))
  
  # Check that file exists
  if (!file.exists(file)) {
    warning(file, " does not exist and was ignored.", call. = FALSE)
    return("")
  }

  # Database name will match that of the zip file
  db.temp <- gsub(".zip", "-temp.db", file)
  db.name <- gsub(".zip", ".db", file)
  
  # Does the file need to be reprocessed? Does it already exist?
  if (file.exists(db.temp)) {
    stop_ifnot_delete(db.temp)
  }
  if (file.exists(db.name)) {
    stop_ifnot_delete(db.name)
  }
  
  # Read list of files in the zip file
  zip.content <- unzip(file, list = TRUE)
  
  # Check that zip file has valid XML and BIN files
  xml.pos <- grep("^Model.*xml$", zip.content$Name)
  bin.pos <- grep("^t_data_[0-4].BIN$", zip.content$Name)
  if ((length(xml.pos) == 0) | (length(bin.pos) == 0)) {
    warning(file, " is not a PLEXOS solution file and was ignored.", call. = FALSE)
    return("")
  }
  
  # Find the XML file and read content
  xml.file <- zip.content[xml.pos, ]
  xml.con <- unz(file, xml.file$Name, open = "r")
  xml.content <- readChar(xml.con, xml.file$Length)
  close(xml.con)
  
  # Check that XML is a valid PLEXOS file
  plexos.check <- grep("SolutionDataset", xml.content)
  if (length(plexos.check) == 0) {
    warning(file, " is not a PLEXOS database and was ignored.", call. = FALSE)
    return("")
  }
  
  # Create an empty database and add the XML information
  message("  - Solution: '", file, "'")
  
  # Open connection to SQLite for R
  dbt <- src_sqlite(db.temp, create = TRUE)
  new_database(dbt, xml.content)
  
  # Add a few tables that will be useful later on
  add_extra_tables(dbt)
  
  # Create SQLite database to store final results
  dbf <- src_sqlite(db.name, create = TRUE)
  times <- c("day", "week", "month", "year")
  start_db(dbf, times)
  
  # Attach final database to temporary database
  dbGetQuery(dbt$con, sprintf("ATTACH '%s' AS new", db.name))
  
  # Add config table
  dbGetQuery(dbt$con, "CREATE TABLE new.config AS SELECT * FROM t_config")
  sql <- sprintf("INSERT INTO new.config VALUES (\"rplexos\", \"%s\")", packageVersion("rplexos"))
  dbGetQuery(dbt$con, sql)
  
  # Add time table
  sql <- "INSERT INTO new.time
          SELECT phase_id, period_id, temp_time
          FROM temp_period_0"
  dbGetQuery(dbt$con, sql)
  
  # Collate information to key (first period data, then summary data)
  sql <- "INSERT INTO new.key
          SELECT k.key_id, c.full_name AS collection, p.name AS property, p.unit,
                 m.name, m.parent_name AS parent, m.category, m.region, m.zone,
                 c.class, c.class_group, k.phase_id,
                 k.period_type_id, k.band_id, k.sample_id, k.timeslice_id
          FROM t_key k
          INNER JOIN temp_membership m
          ON m.membership_id = k.membership_id
          INNER JOIN temp_property p
          ON k.property_id = p.property_id
          INNER JOIN temp_collection c
          ON c.collection_id = p.collection_id
          WHERE k.period_type_id = 0"
  dbGetQuery(dbt$con, sql)
  
  sql <- "INSERT INTO new.key
          SELECT k.key_id, c.full_name AS collection, p.summary_name AS property, p.summary_unit AS unit,
                 m.name, m.parent_name AS parent, m.category, m.region, m.zone,
                 c.class, c.class_group, k.phase_id,
                 k.period_type_id, k.band_id, k.sample_id, k.timeslice_id
          FROM t_key k
          INNER JOIN temp_membership m
          ON m.membership_id = k.membership_id
          INNER JOIN temp_property p
          ON k.property_id = p.property_id
          INNER JOIN temp_collection c
          ON c.collection_id = p.collection_id
          WHERE k.period_type_id = 1"
  dbGetQuery(dbt$con, sql)
  
  # Copy key_interval data
  sql <- "INSERT INTO new.key_interval
          SELECT DISTINCT NULL, collection, name, parent, category, region, zone, class, class_group, phase_id, period_type_id, band_id, sample_id, timeslice_id
          FROM new.key
          WHERE period_type_id = 0"
  dbGetQuery(dbt$con, sql)
  
  # In the temporary file relate both keys
  sql <- "CREATE TABLE temp_key AS
          SELECT * FROM key NATURAL LEFT OUTER JOIN key_interval"
  dbGetQuery(dbt$con, sql)
  
  # Detach database
  dbGetQuery(dbt$con, "DETACH new");
  
  # Create interval data tables and views
  sql <- "SELECT DISTINCT collection, property
        FROM key
        WHERE period_type_id = 0"
  props <- dbGetQuery(dbf$con, sql)
  props$name <- paste0(props$collection, "_", props$property)

  for (p in props$name) {
    view.name  = p
    table.name = paste0("data_interval_", view.name)
  
    sql <- sprintf("CREATE TABLE '%s' (key_interval integer, time_from integer, time_to integer, value double)", table.name)
    dbGetQuery(dbf$con, sql)
    
    sql <- sprintf("CREATE VIEW '%s' AS
                    SELECT k.name, k.parent, k.category, k.region, k.zone, k.class, k.class_group, datetime(t.time) AS time, d.value
                    FROM '%s' d, key_interval k, time t
                    WHERE d.key_interval = k.key_interval AND t.phase_id = k.phase_id
                    AND t.period BETWEEN d.time_from AND d.time_to", view.name, table.name)
    dbGetQuery(dbf$con, sql)
  }
  
  # Add binary data
  for (period in 0:4) {
    # Check if binary file exists, otherwise, skip this period
    bin.name <- sprintf("t_data_%s.BIN", period)
    if(!bin.name %in% zip.content$Name)
      next
    bin.con <- unz(file, bin.name, open = "rb")
    
    # Check if length in t_key_index is correct
    correct.length = correct_length(dbt, period)
    
    if (period > 0) {
      # Read t_key_index entries for current period
      sql <- sprintf("SELECT tki.key_id, nk.phase_id, tki.period_offset, tki.length
                      FROM t_key_index tki
                      JOIN temp_key nk
                      ON tki.key_id = nk.key
                      WHERE tki.period_type_id = %s", period, period)
      t.key <- dbGetQuery(dbt$con, sql)
      
      # Fix length if necessary
      if (!correct.length)
        t.key$length = t.key$length - t.key$period_offset
      
      # Query time
      t.time <- tbl(dbt, sprintf("temp_period_%s", period)) %>%
        arrange(phase_id, period_id) %>%
        select(phase_id, period_id, time = temp_time) %>%
        collect()
      
      # Read all the binary data for summary periods
      t.data <- expand_tkey(t.key)
      t.data$value <- readBin(bin.con, double(),
                              n = nrow(t.data),
                              size = 8,
                              endian = "little")
      t.data2 <- t.data %>%
                 inner_join(t.time, by = c("phase_id", "period_id")) %>%
                 select(key, time, value)
      
      # Execute query in one big binding statement
      sql <- sprintf("INSERT INTO data_%s VALUES(?, ?, ?)", times[period])
      dbBeginTransaction(dbf$con)
      dbGetPreparedQuery(dbf$con, sql, bind.data = t.data2)
      dbCommit(dbf$con)
    } else {
      # Read t_key_index entries for period data
      sql <- "SELECT nk.key_interval, nk.collection, nk.property, tki.period_offset, tki.length
              FROM t_key_index tki
              JOIN temp_key nk
              ON tki.key_id = nk.key
              WHERE tki.period_type_id = 0"
      tki <- dbSendQuery(dbt$con, sql)

      # Data insert in one transaction
      dbBeginTransaction(dbf$con)
      
      # Read one row from the query
      trow <- fetch(tki, 1)
      
      while (nrow(trow) > 0) {
        # Fix length if necessary
        if (!correct.length)
          trow$length = trow$length - trow$period_offset
        
        # Query data
        tdata <- data.frame(key_interval = trow$key_interval,
                            period_id    = 1:trow$length + trow$period_offset)
        tdata$value <- readBin(bin.con, double(),
                               n = nrow(tdata),
                               size = 8,
                               endian = "little")
        
        # Eliminate repeats
        tdata2 <- tdata %>%
          filter(value != lag(value, default = Inf)) %>%
          mutate(period_to_id = lead(period_id - 1, default = max(tdata$period_id)))
        
        # Add data to SQLite
        dbGetPreparedQuery(dbf$con,
                           sprintf("INSERT INTO data_interval_%s_%s (key_interval, time_from, value, time_to)
                                    VALUES(?, ?, ?, ?)", trow$collection, trow$property),
                           bind.data = data.frame(tdata2))
       
        # Next row in tge query
        trow <- fetch(tki, 1)
      }

      # Finish transaction
      dbClearResult(tki)
      dbCommit(dbf$con)
    }
    
    # Close binary file connection
    close(bin.con)
  }
  
  # Close database connections
  dbDisconnect(dbt$con)
  dbDisconnect(dbf$con)
  
  # Delete temporary database
  if (!keep.temp) {
    stop_ifnot_delete(db.temp)
  }
  
  # Return the name of the database that was created
  db.name
}

# Create final database
start_db <- function(db, times) {
  # Store time stamps
  sql <- "CREATE TABLE time (phase_id integer, period integer, time string)"
  dbGetQuery(db$con, sql)
  
  # Store all keys
  sql <- "CREATE TABLE key (key integer PRIMARY KEY, collection string, property string, unit string,
          name string, parent string, category string, region string, zone string, class integer,
          class_group integer, phase_id integer, period_type_id integer, band_id integer, sample_id integer,
          timeslice_id integer)"
  dbGetQuery(db$con, sql)
  
  # Store key for objects, without properties
  sql <- "CREATE TABLE key_interval (key_interval integer PRIMARY KEY, collection string, name string,
          parent string, category string, region string, zone string, class integer, class_group integer,
          phase_id integer, period_type_id integer, band_id integer, sample_id integer, timeslice_id integer)"
  dbGetQuery(db$con, sql)
  
  # For each summary time, create a table and a view
  for (i in times) {
    sql <- sprintf("CREATE TABLE data_%s (key integer, time real, value double)", i);
    dbGetQuery(db$con, sql)
    
    sql <- sprintf("CREATE VIEW %s AS
                    SELECT k.collection, k.property, k.name, k.parent, k.category, k.region, k.zone, k.phase_id,
                    k.period_type_id, k.band_id, k.sample_id, k.timeslice_id, datetime(d.time) AS time, d.value 
                    FROM data_%s d NATURAL LEFT JOIN key k ", i, i);
    dbGetQuery(db$con, sql)
  }
  
  # Create view for list of properties
  sql <- "CREATE VIEW property AS
          SELECT DISTINCT class_group, class, collection, property, unit, phase_id, period_type_id AS is_summary,
                max(band_id) != min(band_id) AS is_multi_band,
                max(sample_id) != min(sample_id) AS is_multi_sample,
                max(timeslice_id) != min(timeslice_id) AS is_multi_timeslice
          FROM key
          GROUP BY class_group, class, collection, property, unit, phase_id, period_type_id
          ORDER BY phase_id, period_type_id, class_group, class, collection, property"
  dbGetQuery(db$con, sql)
        
  # Turn PRAGMA OFF
  dbGetQuery(db$con, "PRAGMA synchronous = OFF")
  dbGetQuery(db$con, "PRAGMA journal_mode = MEMORY")
  dbGetQuery(db$con, "PRAGMA temp_store = MEMORY")
}

# Populate new database with XML contents
new_database <- function(db, xml) {
  # Read XML and convert to a list
  xml.list <- process_xml(xml)
  
  # Turn PRAGMA OFF
  dbGetQuery(db$con, "PRAGMA synchronous = OFF");
  dbGetQuery(db$con, "PRAGMA journal_mode = MEMORY");
  dbGetQuery(db$con, "PRAGMA temp_store = MEMORY");
  
  # Create data tables
  for (i in 0:4) {
    sql <- sprintf("CREATE TABLE t_data_%s (key_id integer, period_id integer, value double)", i)
    dbGetQuery(db$con, sql)
  }
  
  # Create phase tables
  for (i in 0:4) {
    sql <- sprintf("CREATE TABLE t_phase_%s (interval_id integer, period_id integer)", i)
    dbGetQuery(db$con, sql)
  }
  
  # Create t_key_index table
  dbGetQuery(db$con, "CREATE TABLE t_key_index (key_id integer, period_type_id integer, position long, length integer, period_offset integer)");
  
  # Write tables from XML file
  for (t in names(xml.list))
    dbWriteTable(db$con, t, xml.list[[t]], append = TRUE, row.names = FALSE)
  
  0
}

# Add a few tables that will be useful later on
add_extra_tables <- function(db) {
  # Create new table to hold collection names
  #   Special characters are eliminated
  #   full_name includes parent class info or complement name
  sql <- "SELECT c1.*, c3.name AS class, c4.name AS class_group, c2.name AS parent_class, c5.name AS parent_class_group
          FROM t_collection c1
          INNER JOIN t_class c2
          ON c2.class_id == c1.parent_class_id
          INNER JOIN t_class c3
          ON c3.class_id == c1.child_class_id
          INNER JOIN t_class_group c4
          ON c4.class_group_id == c3.class_group_id
          INNER JOIN t_class_group c5
          ON c5.class_group_id == c2.class_group_id"
  col <- dbGetQuery(db$con, sql)
  
  col$name         <- safe_clean_string(col, "name")
  col$parent_class <- safe_clean_string(col, "parent_class")

  col$full_name <- col$name
  sel <- col$parent_class != "System"
  col$full_name[sel] <- paste(col$parent_class, col$name, sep = "_")[sel]

  dbWriteTable(db$con, "temp_collection", col)
  
  # Add category to object
  sql <- "CREATE TABLE temp_object AS
          SELECT o.*, c.name AS category
          FROM t_object o
          INNER JOIN t_category c
          WHERE o.category_id = c.category_id"
  dbGetQuery(db$con, sql)
  
  # Create table with memberships and parent/object information
  sql <- "CREATE TABLE temp_membership0 AS
          SELECT m.*, co.name, po.name AS parent_name, co.category, po.category AS parent_category
          FROM t_membership m
          INNER JOIN temp_object co
          ON co.object_id = m.child_object_id
          INNER JOIN temp_object po
          ON po.object_id = m.parent_object_id"
  dbGetQuery(db$con, sql)
  
  # Add region and zone to memberships
  regs.col <- tbl(db, "temp_collection") %>%
              filter(name == "Generators", parent_class == "Region") %>%
              select(collection_id)
  col.id <- collect(regs.col)$collection_id[1]
  sql <- "CREATE TABLE temp_membership AS
          SELECT m1.*, ifnull(m2.parent_name, '') AS region, ifnull(m2.parent_category, '') AS zone
          FROM temp_membership0 m1
          LEFT OUTER JOIN (SELECT child_object_id, parent_name, parent_category
                           FROM temp_membership0
                           WHERE collection_id = %s) m2
          ON m1.child_object_id = m2.child_object_id"
  sql <- sprintf(sql, col.id)
  dbGetQuery(db$con, sql)
  
  # Create table with property information, including units
  sql <- "SELECT p.*, u.value AS unit, su.value AS summary_unit
          FROM t_property p
          INNER JOIN t_unit u
          ON p.unit_id = u.unit_id
          INNER JOIN t_unit su
          ON p.summary_unit_id = su.unit_id"
  prop <- dbGetQuery(db$con, sql)
  prop$name         <- safe_clean_string(prop, "name")
  prop$summary_name <- safe_clean_string(prop, "summary_name")
  dbWriteTable(db$con, "temp_property", prop)
  
  # Create tables to hold interval, day, week, month, and yearly timestamps
  for (i in 0:4) {
    sql <- sprintf("CREATE TABLE temp_period_%s (phase_id, period_id integer, temp_time real)", i)
    dbGetQuery(db$con, sql)
  }
  
  # For each phase add corresponding values to the time tables
  column.times <- c("day_id", "week_id", "month_id", "fiscal_year_id")
  for (phase in 1:4) {
    # Join t_period_0 and t_phase
    sql <- sprintf("CREATE TABLE temp_phase_%s AS
                    SELECT p.*, ph.period_id, julianday(p.year || '-' || substr(0 || p.month_of_year, -2)
                    || '-' || substr(0 || p.day_of_month, -2) || 'T' || substr(p.datetime, -8)) AS temp_time
                    FROM t_period_0 p
                    INNER JOIN t_phase_%s ph
                    ON p.interval_id = ph.interval_id", phase, phase)
    dbGetQuery(db$con, sql)
    
    # Fix time stamps in t_period_0 (interval)
    sql <- sprintf("INSERT INTO temp_period_0
                    SELECT %s, period_id, temp_time
                    FROM temp_phase_%s", phase, phase)
    dbGetQuery(db$con, sql)
    
    # Fix time stamps in t_period_X (summary data)
    for (i in 1:length(column.times)) {
      sql <- sprintf("INSERT INTO temp_period_%s
                      SELECT %s, %s, min(temp_time)
                      FROM temp_phase_%s
                      GROUP BY %s", i, phase, column.times[i], phase, column.times[i])
      dbGetQuery(db$con, sql)
    }
  }
  
  0
}

# Does t_key_index have the correct length?
correct_length <- function(db, period) {
  q <- sprintf("SELECT max(position / 8 + length) AS JustLength,
                max(position / 8 + length - period_offset) AS LengthMinusOffset,
                sum(length) AS SumLength,
                sum(length - period_offset) AS SumLengthMinusOffset
                FROM t_key_index
                WHERE period_type_id = %s", period)
  res <- dbGetQuery(db$con, q)
  
  out <- TRUE
  
  if (res$JustLength == res$SumLength) {
    out <- TRUE
  } else if (res$LengthMinusOffset == res$SumLengthMinusOffset) {
    out <- FALSE
  } else {
    warning("Problem with lenght of t_key_index for period ", period)
  }
  
  out
}
