# =============================================================================
# fetch_assault_data.R
# Generates assault_data.json for the NYC Assault Dashboard
#
# Run this script each quarter to refresh the data.
# From RStudio: open this file, paste your token below, click Source
#
# Requires: install.packages(c("httr", "jsonlite", "dplyr"))
#
# Output: assault_data.json (place in same folder as assault_dashboard.html)
#
# COLUMNAR FORMAT:
# Each table is stored as { cols: [...], rows: [[...], [...], ...] }
# rather than an array of objects. This eliminates repeated field names
# and reduces file size by ~60-70% compared to object-per-row format.
#
# DATA STRUCTURE:
#   monthly_simple          — year/month/offense/location/borough
#   arrests_monthly_simple  — same for arrests
#   complaints_susp         — yearly + venue + time + suspect demographics
#   complaints_vic          — yearly + venue + time + victim demographics
#   complaints_io_susp      — yearly + inside/outside + suspect demographics
#   complaints_io_vic       — yearly + inside/outside + victim demographics
#   arrests                 — yearly simple perp demographics
#   precinct_map            — nested, for map rendering
# =============================================================================

library(httr)
library(jsonlite)
library(dplyr)

APP_TOKEN <- ""

COMPLAINT_HISTORIC <- "https://data.cityofnewyork.us/resource/qgea-i56i.json"
COMPLAINT_YTD      <- "https://data.cityofnewyork.us/resource/5uac-w243.json"
ARREST_HISTORIC    <- "https://data.cityofnewyork.us/resource/8h9b-rp9u.json"
ARREST_YTD         <- "https://data.cityofnewyork.us/resource/uip8-fykc.json"

current_year    <- 2025
current_quarter <- 4
ytd_end_month   <- current_quarter * 3

cat(sprintf("Data set to: %d Q%d  |  YTD through month %d\n",
            current_year, current_quarter, ytd_end_month))

# ── Definitions ───────────────────────────────────────────────────────────────
FELONY_ASSAULT <- "FELONY ASSAULT"
MISD_ASSAULT   <- "ASSAULT 3 & RELATED OFFENSES"

PD_DESC_MAP <- c(
  "ASSAULT 2,1,UNCLASSIFIED"              = "ASSAULT 2,1,UNCLASSIFIED",
  "STRANGULATION 1ST"                     = "STRANGULATION 1ST",
  "ASSAULT POLICE/PEACE OFFICER"          = "ASSAULT ON POLICE/PEACE OFFICER",
  "ASSAULT 2,1,PEACE OFFICER"             = "ASSAULT ON POLICE/PEACE OFFICER",
  "ASSAULT OTHER PUBLIC SERVICE EMPLOYEE" = "ASSAULT ON OTHER PUBLIC SERVANT",
  "ASSAULT TRAFFIC AGENT"                 = "ASSAULT ON OTHER PUBLIC SERVANT",
  "ASSAULT SCHOOL SAFETY AGENT"           = "ASSAULT ON OTHER PUBLIC SERVANT",
  "ASSAULT 3"                             = "ASSAULT 3",
  "OBSTR BREATH/CIRCUL"                   = "OBSTR BREATH/CIRCUL",
  "MENACING,UNCLASSIFIED"                 = "MENACING",
  "MENACING,PEACE OFFICER"                = "MENACING"
)

PATROL_BORO_MAP <- c(
  "PATROL BORO BRONX"         = "Bronx",
  "PATROL BORO BKLYN NORTH"   = "Brooklyn",
  "PATROL BORO BKLYN SOUTH"   = "Brooklyn",
  "PATROL BORO MAN NORTH"     = "Manhattan",
  "PATROL BORO MAN SOUTH"     = "Manhattan",
  "PATROL BORO QUEENS NORTH"  = "Queens",
  "PATROL BORO QUEENS SOUTH"  = "Queens",
  "PATROL BORO STATEN ISLAND" = "Staten Island"
)

ARREST_BORO_MAP <- c(
  "B"="Bronx","K"="Brooklyn","M"="Manhattan","Q"="Queens","S"="Staten Island"
)

PREM_GROUP_MAP <- list(
  "Subway"            = c("TRANSIT - NYC SUBWAY"),
  "Homeless shelter"  = c("HOMELESS SHELTER"),
  "Park / Playground" = c("PARK/PLAYGROUND"),
  "Hospital"          = c("HOSPITAL"),
  "K-12 Schools"      = c("PUBLIC SCHOOL","PRIVATE/PAROCHIAL SCHOOL"),
  "Residential"       = c("RESIDENCE - APT. HOUSE","RESIDENCE-HOUSE","MAILBOX INSIDE"),
  "Public housing"    = c("RESIDENCE - PUBLIC HOUSING"),
  "Commercial"        = c("ATM","BANK","BAR/NIGHT CLUB","BEAUTY & NAIL SALON","BOOK/CARD",
    "CANDY STORE","CHAIN STORE","CHECK CASHING BUSINESS","CLOTHING/BOUTIQUE",
    "COMMERCIAL BUILDING","DEPARTMENT STORE","DOCTOR/DENTIST OFFICE","DRUG STORE",
    "DRY CLEANER/LAUNDRY","FAST FOOD","FOOD SUPERMARKET","GAS STATION","GROCERY/BODEGA",
    "GYM/FITNESS FACILITY","HOTEL/MOTEL","JEWELRY","LIQUOR STORE","LOAN COMPANY",
    "PHOTO/COPY","REAL ESTATE","RESTAURANT/DINER","SHOE","SMALL MERCHANT","SMOKE SHOP",
    "SOCIAL CLUB/POLICY","STORAGE FACILITY","STORE UNCLASSIFIED","TELECOMM. STORE",
    "VARIETY STORE","VIDEO STORE","MOBILE FOOD","FACTORY/WAREHOUSE"),
  "Other transit"     = c("BUS (NYC TRANSIT)","BUS (OTHER)","BUS STOP","BUS TERMINAL",
    "FERRY/FERRY TERMINAL","TAXI (LIVERY LICENSED)","TAXI (YELLOW LICENSED)",
    "TAXI/LIVERY (UNLICENSED)","TRANSIT FACILITY (OTHER)","TRAMWAY","AIRPORT TERMINAL"),
  "Public / Outdoor"  = c("ABANDONED BUILDING","BRIDGE","HIGHWAY/PARKWAY",
    "OPEN AREAS (OPEN LOTS)","PARKING LOT/GARAGE (PUBLIC)","PARKING LOT/GARAGE (PRIVATE)",
    "STREET","TUNNEL","MARINA/PIER","CEMETERY","MAILBOX OUTSIDE","CONSTRUCTION SITE"),
  "Institutional"     = c("CHURCH","MOSQUE","SYNAGOGUE","OTHER HOUSE OF WORSHIP",
    "COLLEGE/UNIVERSITY","DAYCARE FACILITY","PUBLIC BUILDING"),
  "Unknown / Other"   = c("OTHER","(NULL)","")
)

prem_to_group <- list()
for (grp in names(PREM_GROUP_MAP))
  for (val in PREM_GROUP_MAP[[grp]])
    prem_to_group[[toupper(trimws(val))]] <- grp

VALID_AGE  <- c("<18","18-24","25-44","45-64","65+")
VALID_RACE <- c("BLACK","WHITE HISPANIC","WHITE","BLACK HISPANIC",
                "ASIAN / PACIFIC ISLANDER","AMERICAN INDIAN/ALASKAN NATIVE")
VALID_SEX  <- c("M","F")

# ── Columnar conversion helper ────────────────────────────────────────────────
# Converts a data frame to { cols: [...], rows: [[...], ...] }
# This eliminates repeated field names and cuts file size ~60-70%
to_columnar <- function(df) {
  cols <- names(df)
  rows <- lapply(seq_len(nrow(df)), function(i) {
    unname(as.list(df[i, ]))
  })
  list(cols = cols, rows = rows)
}

# ── Socrata fetch ─────────────────────────────────────────────────────────────
socrata_query_all <- function(endpoint, soql, page_size=50000) {
  all_results <- list(); offset <- 0
  repeat {
    paged <- sprintf("%s LIMIT %d OFFSET %d", soql, page_size, offset)
    qp    <- list(`$query`=paged)
    if (nchar(APP_TOKEN)>0) qp[["$$app_token"]] <- APP_TOKEN
    resp  <- GET(url=endpoint, query=qp,
                 add_headers(Accept="application/json"), timeout(120))
    if (http_error(resp)) stop(paste("API error:", status_code(resp),
                                     content(resp,"text",encoding="UTF-8")))
    batch <- fromJSON(content(resp,"text",encoding="UTF-8"), flatten=TRUE)
    if (is.null(batch)||!is.data.frame(batch)||nrow(batch)==0) break
    all_results[[length(all_results)+1]] <- batch
    if (nrow(batch)<page_size) break
    offset <- offset+page_size; Sys.sleep(0.2)
  }
  if (length(all_results)==0) return(NULL)
  do.call(rbind, all_results)
}

# =============================================================================
# SECTION 1: FETCH COMPLAINTS
# =============================================================================
build_complaint_soql <- function(year) {
  sprintf("SELECT rpt_dt, cmplnt_fr_tm, ofns_desc, pd_desc, law_cat_cd,
            patrol_boro, addr_pct_cd, jurisdiction_code,
            prem_typ_desc, loc_of_occur_desc,
            susp_age_group, susp_race, susp_sex,
            vic_age_group, vic_race, vic_sex
     WHERE  rpt_dt >= '%d-01-01T00:00:00'
       AND  rpt_dt <  '%d-01-01T00:00:00'
       AND  (ofns_desc = 'FELONY ASSAULT'
          OR ofns_desc = 'ASSAULT 3 & RELATED OFFENSES')",
          year, year+1)
}
build_complaint_soql_ytd <- function(year, month_end) {
  sprintf("SELECT rpt_dt, cmplnt_fr_tm, ofns_desc, pd_desc, law_cat_cd,
            patrol_boro, addr_pct_cd, jurisdiction_code,
            prem_typ_desc, loc_of_occur_desc,
            susp_age_group, susp_race, susp_sex,
            vic_age_group, vic_race, vic_sex
     WHERE  rpt_dt >= '%d-01-01T00:00:00'
       AND  rpt_dt <  '%d-01-01T00:00:00'
       AND  date_extract_m(rpt_dt) <= %d
       AND  (ofns_desc = 'FELONY ASSAULT'
          OR ofns_desc = 'ASSAULT 3 & RELATED OFFENSES')",
          year, year+1, month_end)
}

cat("\n=== FETCHING COMPLAINT DATA ===\n")
complaint_rows <- list()
for (yr in 2006:2024) {
  cat(sprintf("  Complaints %d... ", yr))
  result <- tryCatch(socrata_query_all(COMPLAINT_HISTORIC, build_complaint_soql(yr)),
                     error=function(e){cat(sprintf("ERROR: %s\n",e$message));NULL})
  if (!is.null(result)&&nrow(result)>0) {
    complaint_rows[[length(complaint_rows)+1]] <- result
    cat(sprintf("%d rows\n", nrow(result)))
  } else cat("no data\n")
  Sys.sleep(0.3)
}
cat(sprintf("  Complaints %d (YTD Q%d)... ", current_year, current_quarter))
result <- tryCatch(socrata_query_all(COMPLAINT_YTD,
                   build_complaint_soql_ytd(current_year, ytd_end_month)),
                   error=function(e){cat(sprintf("ERROR: %s\n",e$message));NULL})
if (!is.null(result)&&nrow(result)>0) {
  complaint_rows[[length(complaint_rows)+1]] <- result
  cat(sprintf("%d rows\n", nrow(result)))
}
complaints_raw <- do.call(rbind, complaint_rows)
cat(sprintf("Total complaint rows fetched: %d\n", nrow(complaints_raw)))

# ── Process complaints ────────────────────────────────────────────────────────
cat("Processing complaints...\n")
complaints_raw$rpt_dt        <- as.Date(complaints_raw$rpt_dt)
complaints_raw$year          <- as.integer(format(complaints_raw$rpt_dt,"%Y"))
complaints_raw$month         <- as.integer(format(complaints_raw$rpt_dt,"%m"))
complaints_raw$ofns_desc     <- toupper(trimws(as.character(complaints_raw$ofns_desc)))
complaints_raw$pd_desc_raw   <- toupper(trimws(as.character(complaints_raw$pd_desc)))
complaints_raw$patrol_boro   <- toupper(trimws(as.character(complaints_raw$patrol_boro)))
complaints_raw$prem_typ_desc <- toupper(trimws(as.character(complaints_raw$prem_typ_desc)))
complaints_raw$loc_of_occur  <- toupper(trimws(as.character(complaints_raw$loc_of_occur_desc)))
complaints_raw$susp_age      <- toupper(trimws(as.character(complaints_raw$susp_age_group)))
complaints_raw$susp_race     <- toupper(trimws(as.character(complaints_raw$susp_race)))
complaints_raw$susp_sex      <- toupper(trimws(as.character(complaints_raw$susp_sex)))
complaints_raw$vic_age       <- toupper(trimws(as.character(complaints_raw$vic_age_group)))
complaints_raw$vic_race      <- toupper(trimws(as.character(complaints_raw$vic_race)))
complaints_raw$vic_sex       <- toupper(trimws(as.character(complaints_raw$vic_sex)))
complaints_raw$precinct      <- as.integer(as.numeric(as.character(complaints_raw$addr_pct_cd)))
complaints_raw$juris_code    <- as.integer(as.numeric(as.character(complaints_raw$jurisdiction_code)))

complaints_raw$pd_desc <- PD_DESC_MAP[complaints_raw$pd_desc_raw]
complaints_raw$pd_desc[is.na(complaints_raw$pd_desc)] <- "Unknown"
complaints_raw$borough <- PATROL_BORO_MAP[complaints_raw$patrol_boro]
complaints_raw$borough[is.na(complaints_raw$borough)] <- "Unknown"
complaints_raw$loc_type <- ifelse(complaints_raw$juris_code==1,"Subway",
                           ifelse(complaints_raw$juris_code==2,"Housing","Patrol"))
complaints_raw$inside_outside <- ifelse(
  grepl("^INSIDE$", complaints_raw$loc_of_occur), "Inside",
  ifelse(grepl("OUTSIDE|FRONT OF|REAR OF|OPPOSITE OF|IN STREET|SIDEWALK",
               complaints_raw$loc_of_occur), "Outside", "Unknown"))
complaints_raw$prem_group <- sapply(complaints_raw$prem_typ_desc, function(p) {
  grp <- prem_to_group[[p]]; if(is.null(grp)||is.na(grp)) "Unknown / Other" else grp })
complaints_raw$hour <- suppressWarnings(
  as.integer(substr(as.character(complaints_raw$cmplnt_fr_tm),1,2)))
complaints_raw$time_of_day <- ifelse(is.na(complaints_raw$hour),"Unknown",
  ifelse(complaints_raw$hour<6,"Late night (12am-6am)",
  ifelse(complaints_raw$hour<12,"Morning (6am-12pm)",
  ifelse(complaints_raw$hour<18,"Afternoon (12pm-6pm)","Evening / night (6pm-12am)"))))

complaints_raw$susp_age[!complaints_raw$susp_age %in% VALID_AGE]   <- "Unknown"
complaints_raw$vic_age[!complaints_raw$vic_age   %in% VALID_AGE]   <- "Unknown"
complaints_raw$susp_race[!complaints_raw$susp_race %in% VALID_RACE] <- "Unknown"
complaints_raw$vic_race[!complaints_raw$vic_race   %in% VALID_RACE] <- "Unknown"
complaints_raw$susp_sex[!complaints_raw$susp_sex %in% VALID_SEX]   <- "Unknown"
complaints_raw$vic_sex[!complaints_raw$vic_sex   %in% VALID_SEX]   <- "Unknown"
complaints_raw <- complaints_raw[!is.na(complaints_raw$year)&!is.na(complaints_raw$month)&
                                  complaints_raw$year>=2006,]
cat(sprintf("  Valid rows after cleaning: %d\n", nrow(complaints_raw)))

# =============================================================================
# SECTION 2: FETCH ARRESTS
# =============================================================================
build_arrest_soql <- function(year) {
  sprintf("SELECT arrest_date, ofns_desc, pd_desc, law_cat_cd,
            arrest_boro, arrest_precinct, jurisdiction_code,
            age_group, perp_sex, perp_race
     WHERE  arrest_date >= '%d-01-01T00:00:00'
       AND  arrest_date <  '%d-01-01T00:00:00'
       AND  (ofns_desc = 'FELONY ASSAULT'
          OR ofns_desc = 'ASSAULT 3 & RELATED OFFENSES')",
          year, year+1)
}
build_arrest_soql_ytd <- function(year, month_end) {
  sprintf("SELECT arrest_date, ofns_desc, pd_desc, law_cat_cd,
            arrest_boro, arrest_precinct, jurisdiction_code,
            age_group, perp_sex, perp_race
     WHERE  arrest_date >= '%d-01-01T00:00:00'
       AND  arrest_date <  '%d-01-01T00:00:00'
       AND  date_extract_m(arrest_date) <= %d
       AND  (ofns_desc = 'FELONY ASSAULT'
          OR ofns_desc = 'ASSAULT 3 & RELATED OFFENSES')",
          year, year+1, month_end)
}

cat("\n=== FETCHING ARREST DATA ===\n")
arrest_rows <- list()
for (yr in 2006:2024) {
  cat(sprintf("  Arrests %d... ", yr))
  result <- tryCatch(socrata_query_all(ARREST_HISTORIC, build_arrest_soql(yr)),
                     error=function(e){cat(sprintf("ERROR: %s\n",e$message));NULL})
  if (!is.null(result)&&nrow(result)>0) {
    arrest_rows[[length(arrest_rows)+1]] <- result
    cat(sprintf("%d rows\n", nrow(result)))
  } else cat("no data\n")
  Sys.sleep(0.3)
}
cat(sprintf("  Arrests %d (YTD Q%d)... ", current_year, current_quarter))
result <- tryCatch(socrata_query_all(ARREST_YTD,
                   build_arrest_soql_ytd(current_year, ytd_end_month)),
                   error=function(e){cat(sprintf("ERROR: %s\n",e$message));NULL})
if (!is.null(result)&&nrow(result)>0) {
  arrest_rows[[length(arrest_rows)+1]] <- result
  cat(sprintf("%d rows\n", nrow(result)))
}
arrests_raw <- do.call(rbind, arrest_rows)
cat(sprintf("Total arrest rows fetched: %d\n", nrow(arrests_raw)))

cat("Processing arrests...\n")
arrests_raw$arrest_date <- as.Date(arrests_raw$arrest_date)
arrests_raw$year        <- as.integer(format(arrests_raw$arrest_date,"%Y"))
arrests_raw$month       <- as.integer(format(arrests_raw$arrest_date,"%m"))
arrests_raw$ofns_desc   <- toupper(trimws(as.character(arrests_raw$ofns_desc)))
arrests_raw$pd_desc_raw <- toupper(trimws(as.character(arrests_raw$pd_desc)))
arrests_raw$arrest_boro <- toupper(trimws(as.character(arrests_raw$arrest_boro)))
arrests_raw$age_group   <- toupper(trimws(as.character(arrests_raw$age_group)))
arrests_raw$perp_sex    <- toupper(trimws(as.character(arrests_raw$perp_sex)))
arrests_raw$perp_race   <- toupper(trimws(as.character(arrests_raw$perp_race)))
arrests_raw$precinct    <- as.integer(as.numeric(as.character(arrests_raw$arrest_precinct)))
arrests_raw$juris_code  <- as.integer(as.numeric(as.character(arrests_raw$jurisdiction_code)))

arrests_raw$pd_desc <- PD_DESC_MAP[arrests_raw$pd_desc_raw]
arrests_raw$pd_desc[is.na(arrests_raw$pd_desc)] <- "Unknown"
arrests_raw$borough  <- ARREST_BORO_MAP[arrests_raw$arrest_boro]
arrests_raw$borough[is.na(arrests_raw$borough)] <- "Unknown"
arrests_raw$loc_type <- ifelse(arrests_raw$juris_code==1,"Subway",
                        ifelse(arrests_raw$juris_code==2,"Housing","Patrol"))
arrests_raw$age_group[!arrests_raw$age_group %in% VALID_AGE]   <- "Unknown"
arrests_raw$perp_race[!arrests_raw$perp_race %in% VALID_RACE]  <- "Unknown"
arrests_raw$perp_sex[!arrests_raw$perp_sex   %in% VALID_SEX]   <- "Unknown"
arrests_raw <- arrests_raw[!is.na(arrests_raw$year)&!is.na(arrests_raw$month)&
                             arrests_raw$year>=2006,]
cat(sprintf("  Valid rows after cleaning: %d\n", nrow(arrests_raw)))

# =============================================================================
# SECTION 3: AGGREGATIONS
# =============================================================================
cat("\n=== BUILDING AGGREGATIONS ===\n")

# Monthly simple (trend chart)
cat("  monthly_simple...\n")
monthly_simple <- complaints_raw %>%
  group_by(year,month,ofns_desc,loc_type,borough) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(monthly_simple)))

arrests_monthly_simple <- arrests_raw %>%
  group_by(year,month,ofns_desc,loc_type,borough) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    arrests monthly: %d rows\n", nrow(arrests_monthly_simple)))

# Complaints suspect yearly
cat("  complaints_susp_yearly...\n")
complaints_susp <- complaints_raw %>%
  group_by(year,ofns_desc,loc_type,borough,pd_desc,prem_group,time_of_day,
           susp_age,susp_race,susp_sex) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(complaints_susp)))

# Complaints victim yearly
cat("  complaints_vic_yearly...\n")
complaints_vic <- complaints_raw %>%
  group_by(year,ofns_desc,loc_type,borough,pd_desc,prem_group,time_of_day,
           vic_age,vic_race,vic_sex) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(complaints_vic)))

# Inside/outside + suspect
cat("  complaints_io_susp_yearly...\n")
complaints_io_susp <- complaints_raw %>%
  group_by(year,ofns_desc,loc_type,borough,inside_outside,
           susp_age,susp_race,susp_sex) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(complaints_io_susp)))

# Inside/outside + victim
cat("  complaints_io_vic_yearly...\n")
complaints_io_vic <- complaints_raw %>%
  group_by(year,ofns_desc,loc_type,borough,inside_outside,
           vic_age,vic_race,vic_sex) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(complaints_io_vic)))

# Arrests yearly
cat("  arrests_yearly...\n")
arrests_yr <- arrests_raw %>%
  group_by(year,ofns_desc,loc_type,borough,pd_desc,age_group,perp_race,perp_sex) %>%
  summarise(n=n(),.groups="drop")
cat(sprintf("    %d rows\n", nrow(arrests_yr)))

# Precinct map
cat("  precinct maps...\n")
build_precinct_map <- function(raw_df) {
  df <- raw_df %>%
    filter(!is.na(precinct),precinct>0) %>%
    group_by(ofns_desc,loc_type,precinct,year,month) %>%
    summarise(n=n(),.groups="drop")
  all_a <- df %>%
    group_by(loc_type,precinct,year,month) %>%
    summarise(n=sum(n),.groups="drop") %>%
    mutate(ofns_desc="All assaults")
  df <- bind_rows(df,all_a)
  out <- list()
  for (i in seq_len(nrow(df))) {
    of<-df$ofns_desc[i];lt<-df$loc_type[i]
    pct<-paste0("pct_",df$precinct[i])
    yr<-as.character(df$year[i]);mo<-as.character(df$month[i]);n<-df$n[i]
    if(is.null(out[[of]]))out[[of]]<-list()
    if(is.null(out[[of]][[lt]]))out[[of]][[lt]]<-list()
    if(is.null(out[[of]][[lt]][[pct]]))out[[of]][[lt]][[pct]]<-list()
    if(is.null(out[[of]][[lt]][[pct]][[yr]]))out[[of]][[lt]][[pct]][[yr]]<-list()
    out[[of]][[lt]][[pct]][[yr]][[mo]]<-n
  }
  out
}
c_pct_map <- build_precinct_map(complaints_raw)
a_pct_map <- build_precinct_map(arrests_raw)
cat("  precinct maps done.\n")

# ── Size summary ──────────────────────────────────────────────────────────────
total_rows <- nrow(monthly_simple)+nrow(arrests_monthly_simple)+
              nrow(complaints_susp)+nrow(complaints_vic)+
              nrow(complaints_io_susp)+nrow(complaints_io_vic)+nrow(arrests_yr)

cat(sprintf("\n=== SIZE SUMMARY (before columnar conversion) ===\n"))
cat(sprintf("  monthly_simple:          %7d rows\n", nrow(monthly_simple)))
cat(sprintf("  arrests_monthly_simple:  %7d rows\n", nrow(arrests_monthly_simple)))
cat(sprintf("  complaints_susp:         %7d rows\n", nrow(complaints_susp)))
cat(sprintf("  complaints_vic:          %7d rows\n", nrow(complaints_vic)))
cat(sprintf("  complaints_io_susp:      %7d rows\n", nrow(complaints_io_susp)))
cat(sprintf("  complaints_io_vic:       %7d rows\n", nrow(complaints_io_vic)))
cat(sprintf("  arrests:                 %7d rows\n", nrow(arrests_yr)))
cat(sprintf("  TOTAL:                   %7d rows\n", total_rows))
cat(sprintf("\nColumnar format estimated size: ~%.0f MB uncompressed\n",
            total_rows * 30 / 1e6))  # ~30 bytes per row in columnar vs ~120 in object
cat(sprintf("Estimated compressed: ~%.0f MB\n", total_rows * 30 / 1e6 * 0.15))
cat(sprintf("\nPress Enter to write JSON, Ctrl+C to abort...\n"))
readline()

# ── Dropdown values ───────────────────────────────────────────────────────────
felony_pd <- sort(unique(c(
  complaints_susp$pd_desc[complaints_susp$ofns_desc==FELONY_ASSAULT&complaints_susp$pd_desc!="Unknown"],
  arrests_yr$pd_desc[arrests_yr$ofns_desc==FELONY_ASSAULT&arrests_yr$pd_desc!="Unknown"])))
misd_pd <- sort(unique(c(
  complaints_susp$pd_desc[complaints_susp$ofns_desc==MISD_ASSAULT&complaints_susp$pd_desc!="Unknown"],
  arrests_yr$pd_desc[arrests_yr$ofns_desc==MISD_ASSAULT&arrests_yr$pd_desc!="Unknown"])))
susp_races <- sort(unique(complaints_susp$susp_race[complaints_susp$susp_race!="Unknown"]))
vic_races  <- sort(unique(complaints_vic$vic_race[complaints_vic$vic_race!="Unknown"]))
perp_races <- sort(unique(arrests_yr$perp_race[arrests_yr$perp_race!="Unknown"]))

# =============================================================================
# SECTION 4: POPULATION
# =============================================================================
cat("\nDownloading precinct population...\n")
pop_resp <- tryCatch(GET("https://raw.githubusercontent.com/jkeefe/census-by-precincts/master/data/nyc/nyc_precinct_2020pop.csv",timeout(30)),error=function(e)NULL)
precinct_population <- list()
if (!is.null(pop_resp)&&status_code(pop_resp)==200) {
  pop_csv <- read.csv(text=content(pop_resp,"text",encoding="UTF-8"))
  for (i in seq_len(nrow(pop_csv)))
    precinct_population[[as.character(pop_csv$precinct[i])]] <- as.integer(pop_csv$P1_001N[i])
  cat(sprintf("  Loaded %d precincts\n", length(precinct_population)))
}

NYC_POP <- list(
  "2006"=list(nyc=8214426,Bronx=1364566,Brooklyn=2505040,Manhattan=1596843,Queens=2270338,`Staten Island`=472639),
  "2007"=list(nyc=8274527,Bronx=1384115,Brooklyn=2528799,Manhattan=1604776,Queens=2285116,`Staten Island`=471721),
  "2008"=list(nyc=8333346,Bronx=1397751,Brooklyn=2551026,Manhattan=1621279,Queens=2290514,`Staten Island`=472776),
  "2009"=list(nyc=8391881,Bronx=1410657,Brooklyn=2572671,Manhattan=1634877,Queens=2303200,`Staten Island`=470476),
  "2010"=list(nyc=8175133,Bronx=1385108,Brooklyn=2504700,Manhattan=1585873,Queens=2230722,`Staten Island`=468730),
  "2011"=list(nyc=8244910,Bronx=1398975,Brooklyn=2532645,Manhattan=1601948,Queens=2237644,`Staten Island`=473698),
  "2012"=list(nyc=8336697,Bronx=1420695,Brooklyn=2565635,Manhattan=1619268,Queens=2253615,`Staten Island`=477484),
  "2013"=list(nyc=8405837,Bronx=1432132,Brooklyn=2595259,Manhattan=1626159,Queens=2272771,`Staten Island`=479516),
  "2014"=list(nyc=8491079,Bronx=1446788,Brooklyn=2629150,Manhattan=1636268,Queens=2296177,`Staten Island`=482696),
  "2015"=list(nyc=8550405,Bronx=1455720,Brooklyn=2649049,Manhattan=1643734,Queens=2312476,`Staten Island`=476426),
  "2016"=list(nyc=8537673,Bronx=1455919,Brooklyn=2641052,Manhattan=1644518,Queens=2321580,`Staten Island`=474604),
  "2017"=list(nyc=8622698,Bronx=1471160,Brooklyn=2678884,Manhattan=1664727,Queens=2339150,`Staten Island`=476777),
  "2018"=list(nyc=8398748,Bronx=1432132,Brooklyn=2600747,Manhattan=1628706,Queens=2278906,`Staten Island`=476253),
  "2019"=list(nyc=8336817,Bronx=1418207,Brooklyn=2576771,Manhattan=1628701,Queens=2253858,`Staten Island`=475280),
  "2020"=list(nyc=8804190,Bronx=1472654,Brooklyn=2736074,Manhattan=1694251,Queens=2405464,`Staten Island`=495747),
  "2021"=list(nyc=8467513,Bronx=1379946,Brooklyn=2590516,Manhattan=1596273,Queens=2278029,`Staten Island`=422749),
  "2022"=list(nyc=8335897,Bronx=1356476,Brooklyn=2561225,Manhattan=1597451,Queens=2278029,`Staten Island`=422716),
  "2023"=list(nyc=8258035,Bronx=1336705,Brooklyn=2530151,Manhattan=1596909,Queens=2245398,`Staten Island`=418872),
  "2024"=list(nyc=8258035,Bronx=1336705,Brooklyn=2530151,Manhattan=1596909,Queens=2245398,`Staten Island`=418872),
  "2025"=list(nyc=8258035,Bronx=1336705,Brooklyn=2530151,Manhattan=1596909,Queens=2245398,`Staten Island`=418872)
)

# =============================================================================
# SECTION 5: WRITE JSON (columnar format)
# =============================================================================
cat("\nConverting to columnar format and writing JSON...\n")

output <- list(
  meta = list(
    generated       = format(Sys.time(),"%Y-%m-%dT%H:%M:%S"),
    current_year    = current_year,
    current_quarter = paste0("Q",current_quarter),
    ytd_through     = sprintf("End of Q%d (%d months)",current_quarter,ytd_end_month),
    format          = "columnar",
    note            = "Tables stored as {cols:[...], rows:[[...],...]} for compact size."
  ),
  filters = list(
    offense_types  = list(all="All assaults",felony=FELONY_ASSAULT,misd=MISD_ASSAULT),
    pd_desc        = list(felony=felony_pd,misd=misd_pd),
    loc_types      = c("Patrol","Subway","Housing"),
    boroughs       = c("Bronx","Brooklyn","Manhattan","Queens","Staten Island"),
    prem_groups    = names(PREM_GROUP_MAP),
    inside_outside = c("Inside","Outside","Unknown"),
    time_of_day    = c("Late night (12am-6am)","Morning (6am-12pm)",
                       "Afternoon (12pm-6pm)","Evening / night (6pm-12am)","Unknown"),
    age_groups     = c(VALID_AGE,"Unknown"),
    susp_races     = c(susp_races,"Unknown"),
    vic_races      = c(vic_races,"Unknown"),
    perp_races     = c(perp_races,"Unknown"),
    sexes          = c("M","F","Unknown")
  ),
  population          = NYC_POP,
  precinct_population = precinct_population,
  # All tables in columnar format
  monthly_simple         = to_columnar(monthly_simple),
  arrests_monthly_simple = to_columnar(arrests_monthly_simple),
  complaints_susp        = to_columnar(complaints_susp),
  complaints_vic         = to_columnar(complaints_vic),
  complaints_io_susp     = to_columnar(complaints_io_susp),
  complaints_io_vic      = to_columnar(complaints_io_vic),
  arrests                = to_columnar(arrests_yr),
  precinct_map = list(complaints=c_pct_map, arrests=a_pct_map)
)

out_path <- "assault_data.json"
cat(sprintf("Writing %s...\n", out_path))
write(toJSON(output, auto_unbox=TRUE, null="null"), out_path)

size_kb <- round(file.size(out_path)/1024)
cat(sprintf("\nDone! Wrote %s (%d KB / %.1f MB)\n", out_path, size_kb, size_kb/1024))
cat(sprintf("  Total data rows: %d\n", total_rows))
cat(sprintf("  Estimated compressed: ~%.0f MB\n", size_kb/1024*0.12))
cat("\nPlace assault_data.json + precincts.geojson in same folder as assault_dashboard.html\n")
