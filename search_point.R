require(DBI)
require(rJava)
require(RJDBC)

search_point <- function(sclist, from, ...) {

	prequery <- "select sclientid, ddate, skey, sval from p_point_data_h where"

	incaluse <- paste("'", paste(sclist, collapse = "','"), "'", sep = "")
	midquery <- sprintf("sclientid in (%s) and", incaluse)

	args <- list(...)
	if ( length(args) == 1 ) {
  		query <- sprintf("%s %s to_date(ddate) between '%s' and '%s'", prequery, midquery, from, args[1])
	} else {
  		query <- sprintf("%s %s to_date(ddate) >= '%s'", prequery, midquery, from)
	}

	drv <- JDBC("org.apache.hive.jdbc.HiveDriver", "hive-jdbc-3.1.0.3.1.4.6-1-standalone.jar", identifier.quote="'")
	conn <- dbConnect(drv, "jdbc:hive2://192.168.7.164:10000/default","hive", " !Hive1357")

	query_data <- dbGetQuery(conn, query)
	sorted_data <- query_data[ c(order(query_data$sclientid, query_data$ddate, query_data$skey)), ]
	output <- reshape(sorted_data, timevar="skey", idvar=c("sclientid", "ddate"), direction="wide")
	return(output)
}

#clist <- c('00000220d02544fffefbc4bf', '00000220d02544fffefc768c');
clist <- c('00000220d02544fffefbc4bf');
myData <- search_point(clist, '2019-09-01', '2019-09-12')
#myData <- search_point(clist, '2019-09-01')
myData

