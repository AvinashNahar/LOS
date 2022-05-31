
.libPaths("C:/Program Files/R/R-4.1.2/library")
library(stringr)
library(tidyr)
library(XLConnect)
library(plyr)
library(dplyr)
library(lubridate)
library(janitor)
library(data.table)


#---------------------------Input Directory------------------------

setwd("C:/Users/bischeduler/Documents/Codes_to_be_Scheduled/LOS")

wb <- loadWorkbook('LOS_VSJ_SummaryChecknew.xlsx')
setStyleAction(wb,XLC$"STYLE_ACTION.NONE")
wb1 <- loadWorkbook('StagewiseTemplate.xlsx')
setStyleAction(wb1,XLC$"STYLE_ACTION.NONE")
wb2 <- loadWorkbook('LOS_VSJ_LAPSummaryChecknew.xlsx')
setStyleAction(wb2,XLC$"STYLE_ACTION.NONE")
wb3 <- loadWorkbook('StagewiseTemplate_LAP.xlsx')
setStyleAction(wb3,XLC$"STYLE_ACTION.NONE")

myconn3 <- DBI::dbConnect(odbc::odbc(), "GHF_BI_CONN", uid="GHF_BI_CONN", pwd="Godrej@123")


if (format(Sys.time(), "%a")=="Mon" & format(Sys.time(), "%d")==1) {
  query <- readLines("bigsql_Monday_1stMonth.sql")
  a<- 'File Read for bigsql_Monday_1stMonth.sql'
  print(a)
} else if (format(Sys.time(), "%d")==1) {
  query <-readLines("bigsql_1stMonth_v2.sql")
  a<-"file read for bigsql_1stMonth_v2.sql"
  print(a)
} else if (format(Sys.time(), "%a")=="Mon") {
  query <-readLines("bigsql_Monday.sql")
  a<-"file read for bigsql_Monday.sql"
  print(a)
} else {
  query <-readLines("bigsql_Tuesday_Sunday.sql")
  a<-'Regular file read: bigsql_Tuesday_Sunday.sql '
  print(a)
}

#query <- readLines("bigsql_Tuesday_Sunday.sql")
#bigsql_1stMonth_v2_Extd2ndMay.sql

#------------------------ OUTPUT Directory----------------------------------

setwd("C:/Users/bischeduler/Documents/Output_of_Scheduler/BI_Daily_Reports")
# It will not purge directory if exists
newdir <- paste(Sys.Date(), sep = "-")
dir.create(newdir)
setwd(newdir)


logfile <- paste0("BI_Daily_Logs-", format(Sys.time(), "%d-%b-%Y"), ".log")


cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t ----------- Log file Start ------------ \n", file = logfile, append = TRUE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Connection to Snowflake Successfull \n", file = logfile, append = TRUE)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t ",a," \n", file = logfile, append = TRUE)


one <- "select count(*) from DTCRON;"
mydata <- DBI::dbGetQuery(myconn3,one)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DTCRON Count as of yesterday ", mydata$`COUNT(*)` ,"\n", file = logfile, append = TRUE)


queries1 = str_replace_all(query,'--.*$',"")  ### remove any commented lines
queries2 = paste(queries1, collapse = '\n') ### collapse with new lines
queries3 = unlist(str_split(queries2,"(?<=;)")) ### separate individual queries
#queries3 = queries3[1:209]


cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t SQL Queries Ready to Execute \n", file = logfile, append = TRUE)


for (i in 1:(length(queries3))) {
  print(i)
  cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t SQL Query: ",queries3[i] ," \n", file = "BI_AllQueries.log", append = TRUE)
  DBI::dbGetQuery(myconn3, queries3[i])}

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t ALL SQL Queries Executed Successfully -- 1 \n", file = logfile, append = TRUE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t ALL SQL Queries Executed Successfully -- 2 \n", file = logfile, append = TRUE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t ALL SQL Queries Executed Successfully -- 3 \n", file = logfile, append = TRUE)



#result <- readLines("bigsql_results.txt")
#result1 = str_replace_all(result,'--.*$'," ")  ### remove any commented lines
#result2 = paste(result1, collapse = '\n') ### collapse with new lines
#result3 = unlist(str_split(result2,"(?<=;)")) ### separate individual queries
#result4 = unlist(str_split(result2,"FROM | from")) ### separate individual queries
#unlist(str_split(result3,"FROM | from "))

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Begin Saving CSV files ---  \n", file = logfile, append = TRUE)


#------------------------------Snowflake-Output---------------------------

one <- "select * from LOS_FINAL_SUMMARY;"
LOS_FINAL_SUMMARY <- DBI::dbGetQuery(myconn3,one)
LOS_FINAL_SUMMARY<-replace(LOS_FINAL_SUMMARY,is.na(LOS_FINAL_SUMMARY),0)
fn<-paste0("LOS_SUMMARY",".csv")
write.csv(LOS_FINAL_SUMMARY,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_FINAL_SUMMARY.csv \n", file = logfile, append = TRUE)


#------------------------------VSJ4---------------------------

one <- "select distinct * from VSJ4;"
VSJ4 <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("LOS_VSJ4",".csv")
write.csv(VSJ4,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_VSJ4.csv \n", file = logfile, append = TRUE)


#------------------------------AUM-Trend---------------------------

h<- VSJ4
h<-subset(h,h$LOAN_STATUS == 'Active' & h$STATUS == 'Booked')
m2<-ddply(h,c('FINBRANCH','FINTYPE'),summarise,AUM=sum(PRINCIPAL_OUTSTANDING/10000000))
m2<-spread(m2, FINTYPE, AUM)
m2<-replace(m2,is.na(m2),0)
m2<-m2[,c(1,4,5,7,2,3,6,8)]
# fn<-paste0("AUM_Trend",".csv")
# write.csv(m2,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t AUM_Trend.csv \n", file = logfile, append = TRUE)


#------------------------------VSJ Adoption Rate---------------------------

MTD<- paste(year(today()-1),month(today()-1),sep = "-")
Branch<-sort(unique(VSJ4$FINBRANCH))
vsjrate <-data.frame(Branch)
dd1 <- subset(VSJ4,VSJ4$FINSOURCEID == 'APIUSER' 
              & !VSJ4$RESIDENTIAL_STATUS %in% c('NR','MN','PIO') &
                VSJ4$EMPLOYMENT_TYPE %in% c('SALARIED','NON-WORKING')
              & VSJ4$LOGINSTATUS == 'A) Login' & VSJ4$LOGIN_YEAR_MONTH == MTD)
dd1<-ddply(dd1,'FINBRANCH',summarise,API = length(FINREFERENCE))
vsjrate<- merge(x = vsjrate,y = dd1,by.x = 'Branch',by.y = 'FINBRANCH',all.x = TRUE)
dd2 <- subset(VSJ4,VSJ4$FINTYPE %in% c('HL','HT')
              & !VSJ4$RESIDENTIAL_STATUS %in% c('NR','MN','PIO') &
                VSJ4$EMPLOYMENT_TYPE %in% c('SALARIED','NON-WORKING')
              & VSJ4$LOGINSTATUS == 'A) Login' & VSJ4$LOGIN_YEAR_MONTH == MTD)
dd2<-ddply(dd2,'FINBRANCH',summarise, TotalCount = length(FINREFERENCE))
vsjrate<- merge(x = vsjrate,y = dd2,by.x = 'Branch',by.y = 'FINBRANCH',all.x = TRUE)
vsjrate$adoptionpercent<-vsjrate$API/vsjrate$TotalCount
#vsjrate<-vsjrate[,c(1,2,4,3)]


#------------------------------VSJ4 Null Check File---------------------------

dm <- "select distinct * from VSJ4 where detailed_status is NULL;"
mydata <- DBI::dbGetQuery(myconn3,dm)
fn<-paste0("LOS_VSJ4_Check_null",".csv")
write.csv(mydata,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_VSJ4_Check_null is Correct \n", file = logfile, append = TRUE)

#------------------------------ Check Duplicates in VSJ4----------------------

cd<-"select FINREFERENCE, count(FINREFERENCE) Volume from VSJ4
group by FINREFERENCE
having count(FINREFERENCE) >1"
d <- DBI::dbGetQuery(myconn3,cd)
#d<-data.frame(cd)
#fn<-paste0("LOS_VSJ4_Check_Duplicates",".csv")
write.csv(d,"LOS_VSJ4_Check_Duplicates.csv", row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_VSJ4_Check_Duplicates \n", file = logfile, append = TRUE)


#------------------------------ Check Duplicates in DTCRON----------------------

cd<-"select FINREFERENCE, count(FINREFERENCE) Volume from DTCRON
group by FINREFERENCE
having count(FINREFERENCE) >1"
d <- DBI::dbGetQuery(myconn3,cd)
#d<-data.frame(cd)
#fn<-paste0("LOS_VSJ4_Check_Duplicates",".csv")
write.csv(d,"DTCRON_Check_Duplicates.csv", row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DTCRON_Check_Duplicates \n", file = logfile, append = TRUE)


#------------------------------ Check New Branches in VSJ4----------------------

FINBRANCH<-sort(unique(VSJ4$FINBRANCH))
nb<-data.frame(FINBRANCH)
fn<-paste0("LOS_VSJ4_Check_NewBranch",".csv")
write.csv(nb,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_VSJ4_Check_NewBranch \n", file = logfile, append = TRUE)


#------------------------------Income Sanction Volume--------------------------

one <- "SELECT * FROM ISFLOW_HLVolume;"
IS_VOL <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_ISFLOW_HLVolume",".csv")
# write.csv(IS_VOL,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_ISFLOW_HLVolume \n", file = logfile, append = TRUE)


#------------------------------Income Sanction Value---------------------------

one <- "SELECT * FROM ISFLOW_HLValue;"
IS_VAL <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_ISFLOW_HLValue",".csv")
# write.csv(IS_VAL,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_ISFLOW_HLValue \n", file = logfile, append = TRUE)



#------------------------------First Disbursement---------------------------

one <- "SELECT * FROM V_FIRSTDISB;"
V_FIRSTDISB <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_V_FIRSTDISB",".csv")
# write.csv(V_FIRSTDISB,fn, row.names = FALSE)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_V_FIRSTDISB \n", file = logfile, append = TRUE)


#------------------------------Tranche Disbursement---------------------------

one <- "SELECT * FROM V_DISBTRUNC;"
V_DISBTRUNC <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_V_DISBTRUNC",".csv")
# write.csv(V_DISBTRUNC,fn, row.names = FALSE)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_V_DISBTRUNC \n", file = logfile, append = TRUE)


#------------------------------Fin disbursement Details Dump---------------------------

one <- "SELECT * FROM V_FINDISBURSEMENTDETAILS;"
FINDISB <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_FINDISBURSEMENTDETAILS",".csv")
# write.csv(FINDISB,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_FINDISBURSEMENTDETAILS \n", file = logfile, append = TRUE)


#------------------------------PO View---------------------------

one <- "SELECT * FROM PO_VIEW;"
PO_VIEW <- DBI::dbGetQuery(myconn3,one)
# fn<-paste0("LOS_PO_VIEW",".csv")
# write.csv(PO_VIEW,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_PO_VIEW \n", file = logfile, append = TRUE)



#------------------------------DTCRON---------------------------

one <- "SELECT * FROM DTCRON;"
mydata <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("DTCRON",".csv")
write.csv(mydata,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DTCRON \n", file = logfile, append = TRUE)


#------------------------------StageWise Disbursement Queue---------------------------

one <- "SELECT * FROM DISBQUEUE_SUMMARY;"
DISBQUEUE_SUMMARY <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("LOS_STAGE_DISBQUEUE_SUMMARY",".csv")
write.csv(DISBQUEUE_SUMMARY,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_DISBQUEUE_SUMMARY \n", file = logfile, append = TRUE)


#-------------------------------Stage wise Credit Queue------------------------------------

one <- "SELECT * FROM CREDITQUEUE_SUMMARY;"
CREDITQUEUE_SUMMARY <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("LOS_STAGE_CREDITQUEUE_SUMMARY",".csv")
write.csv(CREDITQUEUE_SUMMARY,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_CREDITQUEUE_SUMMARY \n", file = logfile, append = TRUE)


#------------------------------Stage wise Ops Queue--------------------------

one <- "SELECT * FROM OPSQUEUE_SUMMARY;"
OPSQUEUE_SUMMARY <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("LOS_STAGE_OPSQUEUE_SUMMARY",".csv")
write.csv(OPSQUEUE_SUMMARY,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_OPSQUEUE_SUMMARY \n", file = logfile, append = TRUE)


#------------------------------Sales Query Adoption---------------------------

one <- "SELECT * FROM MIS_QUERY_MANAGEMENT_VIEW_ADOPTION;"
mydata <- DBI::dbGetQuery(myconn3,one)
fn<-paste0("LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION",".csv")
write.csv(mydata,fn, row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION \n", file = logfile, append = TRUE)
dd<-mydata
MTD<- paste(year(today()-1),month(today()-1),sep = "_")
FINBRANCH<-sort(unique(dd$FINBRANCH))
query_view<-data.frame(FINBRANCH)
dd1<-subset(dd,dd$RAISED_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES')
dd1<-ddply(dd1,('FINBRANCH'),summarise,TOTAL_RAISED = length(LAN))
query_view<-merge(x=query_view,y=dd1,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd2<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES')
dd2<-ddply(dd2,('FINBRANCH'),summarise,TOTAL_RESPONDED = length(LAN))
query_view<-merge(x=query_view,y=dd2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd3<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES'
            & dd$RESPONSE_BY_ROLE %in% c('AM','AMCP','AM-LAP','ASM','DSH','SM','ST'))

dd3<-ddply(dd3,('FINBRANCH'),summarise,By_Sales = length(LAN))
query_view<-merge(x=query_view,y=dd3,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
query_view$Adoption_Rate<-query_view$By_Sales/query_view$TOTAL_RESPONDED
query_view1<-replace(query_view,is.na(query_view),0)


#------------------------------ Sales Query Adoption(LP-NP) ---------------------------

one <- "SELECT * FROM MIS_QUERY_MANAGEMENT_VIEW_ADOPTION;"
mydata <- DBI::dbGetQuery(myconn3,one)
#fn<-paste0("LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION",".csv")
#write.csv(mydata,fn, row.names = FALSE)
#cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION \n", file = logfile, append = TRUE)
dd<-mydata
#MTD<- paste(year(today()-1),month(today()-1),sep = "_")
FINBRANCH<-sort(unique(dd$FINBRANCH))
query_view<-data.frame(FINBRANCH)
dd1<-subset(dd,dd$RAISED_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES' & 
              dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan') )
#query_view$TOTAL_RAISED <- 0
if (nrow(dd1) == 0) {query_view$TOTAL_RAISED <- 0} else {
  dd1<-ddply(dd1,('FINBRANCH'),summarise,TOTAL_RAISED = length(LAN))
  query_view<-merge(x=query_view,y=dd1,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                    all.x = TRUE)}
dd2<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES' & 
              dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan'))
if (nrow(dd2) == 0) {query_view$TOTAL_RESPONDED <- 0} else {
  dd2<-ddply(dd2,('FINBRANCH'),summarise,TOTAL_RESPONDED = length(LAN))
  query_view<-merge(x=query_view,y=dd2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                    all.x = TRUE)}
dd3<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES'
            & dd$RESPONSE_BY_ROLE %in% c('AM','AMCP','AM-LAP','ASM','DSH','SM','ST') & 
              dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan'))

if (nrow(dd3) == 0) {query_view$By_Sales <- 0} else {
  dd3<-ddply(dd3,('FINBRANCH'),summarise,By_Sales = length(LAN))
  query_view<-merge(x=query_view,y=dd3,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                    all.x = TRUE)}

query_view$Adoption_Rate<-query_view$By_Sales/query_view$TOTAL_RESPONDED
query_view2<-replace(query_view,is.na(query_view),0)





#------------------------------ Sales Query Adoption W/o(LP-NP) ---------------------------

one <- "SELECT * FROM MIS_QUERY_MANAGEMENT_VIEW_ADOPTION;"
mydata <- DBI::dbGetQuery(myconn3,one)
#fn<-paste0("LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION",".csv")
#write.csv(mydata,fn, row.names = FALSE)
#cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION \n", file = logfile, append = TRUE)
dd<-mydata
#MTD<- paste(year(today()-1),month(today()-1),sep = "_")
FINBRANCH<-sort(unique(dd$FINBRANCH))
query_view<-data.frame(FINBRANCH)
dd1<-subset(dd,dd$RAISED_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES' & 
              !dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan') )
dd1<-ddply(dd1,('FINBRANCH'),summarise,TOTAL_RAISED = length(LAN))
query_view<-merge(x=query_view,y=dd1,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd2<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES' & 
              !dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan'))
dd2<-ddply(dd2,('FINBRANCH'),summarise,TOTAL_RESPONDED = length(LAN))
query_view<-merge(x=query_view,y=dd2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd3<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD & dd$RAISE_BY_ROLE == 'CRM'
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES'
            & dd$RESPONSE_BY_ROLE %in% c('AM','AMCP','AM-LAP','ASM','DSH','SM','ST') & 
              !dd$LOANTYPE %in% c('Loan Against Property','Non-Residential Property Loan'))

dd3<-ddply(dd3,('FINBRANCH'),summarise,By_Sales = length(LAN))
query_view<-merge(x=query_view,y=dd3,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
query_view$Adoption_Rate<-query_view$By_Sales/query_view$TOTAL_RESPONDED
query_view3<-replace(query_view,is.na(query_view),0)


#------------------------------Sales Query Adoption(Ops)---------------------------

one <- "SELECT * FROM MIS_QUERY_MANAGEMENT_VIEW_ADOPTION;"
mydata <- DBI::dbGetQuery(myconn3,one)
#fn<-paste0("LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION",".csv")
#write.csv(mydata,fn, row.names = FALSE)
#cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_MIS_QUERY_MANAGEMENT_VIEW_ADOPTION \n", file = logfile, append = TRUE)
dd<-mydata
#MTD<- paste(year(today()-1),month(today()-1),sep = "_")
FINBRANCH<-sort(unique(dd$FINBRANCH))
query_view<-data.frame(FINBRANCH)
dd1<-subset(dd,dd$RAISED_ON_YEAR_MONTH %in% MTD 
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES')
dd1<-dd1%>% filter(QUERIES_RAISED_BY %like% '^BR' | QUERIES_RAISED_BY %like% '^DDE')
dd1<-ddply(dd1,('FINBRANCH'),summarise,TOTAL_RAISED = length(LAN))
query_view<-merge(x=query_view,y=dd1,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd2<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES')
dd2<-dd2%>% filter(QUERIES_RAISED_BY %like% '^BR' | QUERIES_RAISED_BY %like% '^DDE')
dd2<-ddply(dd2,('FINBRANCH'),summarise,TOTAL_RESPONDED = length(LAN))
query_view<-merge(x=query_view,y=dd2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
dd3<-subset(dd,dd$RESPONSE_ON_YEAR_MONTH %in% MTD
            & dd$CATEGORY == 'SALES' & dd$ASSIGNED_ROLL == 'SALES'
            & dd$RESPONSE_BY_ROLE %in% c('AM','AMCP','AM-LAP','ASM','DSH','SM','ST'))
dd3<-dd3%>% filter(QUERIES_RAISED_BY %like% '^BR' | QUERIES_RAISED_BY %like% '^DDE')
dd3<-ddply(dd3,('FINBRANCH'),summarise,By_Sales = length(LAN))
query_view<-merge(x=query_view,y=dd3,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
query_view$Adoption_Rate<-query_view$By_Sales/query_view$TOTAL_RESPONDED
query_view4<-replace(query_view,is.na(query_view),0)

rework_per<-read.csv("rework.csv",header = T)

#----------------------Writing as new LOS Dashboard------------------

writeWorksheet(wb,sheet = 'snowflake output',LOS_FINAL_SUMMARY)
#deleteData(wb,sheet = 'IS flow table', cols = 1:240, rows = 1:240, gridExpand = FALSE)
writeWorksheet(wb,sheet = 'ISVol',IS_VOL)
writeWorksheet(wb,sheet = 'ISVal',IS_VAL,startCol = 1,
               startRow = 1)
#deleteData(wb,sheet = 'aum table', cols = 1:240, rows = 1:240, gridExpand = FALSE)
writeWorksheet(wb,sheet = 'aum table',m2,startCol = 1,
               startRow = 1)
#deleteData(wb,sheet = 'tranche dump', cols = 1:240, rows = 1:240, gridExpand = FALSE)
writeWorksheet(wb,sheet = 'tranche dump',V_FIRSTDISB,startCol = 1,
               startRow = 2)
writeWorksheet(wb,sheet = 'tranche dump',V_DISBTRUNC,startCol = 1,
               startRow = 18)
#deleteData(wb,sheet = 'PO_VIEW', cols = 1:240, rows = 1:240, gridExpand = FALSE)
writeWorksheet(wb,sheet = 'PO_VIEW',PO_VIEW,startCol = 1,
               startRow = 1)
writeWorksheet(wb,query_view1,sheet = 'query',startRow = 4,startCol = 1,header = FALSE)
writeWorksheet(wb,query_view3,sheet = 'query',startRow = 4,startCol = 7,header = FALSE)
writeWorksheet(wb,query_view2,sheet = 'query',startRow = 4,startCol = 13,header = FALSE)
writeWorksheet(wb,query_view4,sheet = 'query',startRow = 4,startCol = 20,header = FALSE)
writeWorksheet(wb,vsjrate,sheet = 'vsj',startRow = 4,startCol = 1,header = FALSE)
writeWorksheet(wb,vsjrate,sheet = 'Summary',startRow = 58,startCol = 3,header = FALSE)
writeWorksheet(wb,rework_per,sheet = 'Rework_percentage',startRow = 5,startCol = 4,header = FALSE)


setForceFormulaRecalculation(wb, sheet = '*', TRUE)

#------------------------------Saving LOS VSJ Dashboard--------------------------

saveWorkbook(wb,"LOS_GC_SUMMARY.xlsx")

#-------------------------------Writing LOS_LAP Dashboard-----------
writeWorksheet(wb2,sheet = 'snowflake output',LOS_FINAL_SUMMARY)
writeWorksheet(wb2,sheet = 'aum table',m2,startCol = 1,
               startRow = 1)

setForceFormulaRecalculation(wb2, sheet = '*', TRUE)

#------------------------------Saving LOS LAP Dashboard--------------------------

saveWorkbook(wb2,"LOS_LAP_SUMMARY.xlsx")


#------------------------------RCU MIS---------------------------

q<-'select * from MIS_RCU_VIEW'
mydata <- DBI::dbGetQuery(myconn3,q)
fn<-paste0("MIS_RCU_VIEW.csv")
write.csv(mydata,fn,row.names = F)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MIS_RCU_VIEW.csv \n", file = logfile, append = TRUE)


#-------------------------SRM MIS ---------

q <- 'select * from mis_service_request_view'
SRM <- DBI::dbGetQuery(myconn3,q)
write.csv(SRM,'SRM_MIS.csv',row.names = F)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MIS_SRM.csv \n", file = logfile, append = TRUE)


#--------------- Allocation Report ---------
q<-"SELECT A.FINREFERENCE, A.NEXTROLECODE, A.ROLECODE, A.RECORDSTATUS, A.FINBRANCH, A.DETAILED_STATUS, A.STATUS, A.FINTYPE,
(A.REQLOANAMT/100) AS REQLOANAMT, A.EMPLOYMENT_TYPE, A.CIF , A.FIRST_CREDIT_MANAGER,
B.FINREFERENCE AS OTHER_LAN, B.FIRST_CREDIT_MANAGER AS OTHER_FIRST_CREDIT_MANAGER
FROM DTCRON A
LEFT JOIN (SELECT LISTAGG(FINREFERENCE,', ') AS FINREFERENCE, CIF,
LISTAGG(FIRST_CREDIT_MANAGER,', ') AS FIRST_CREDIT_MANAGER
FROM DTCRON
WHERE FINTYPE IN ('FL','FT','HT','LT')
GROUP BY CIF) B
ON A.CIF = B.CIF AND A.FINREFERENCE <> B.FINREFERENCE
WHERE FINTYPE IN ('HL','HT','LT','FL','FT') AND STATUS IN ('DDE','CO','CM & Above')
ORDER BY A.CIF;"
AR <- DBI::dbGetQuery(myconn3,q)
write.csv(AR,'Allocation_Report.csv',row.names = F)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Allocation_Report.csv \n", file = logfile, append = TRUE)

#-----------------------------Mumbai Data(Urang)---------------------------

mu<-'select * from FS_FTD'
FS_FTD <- DBI::dbGetQuery(myconn3,mu)
#fn<-paste0("MUMBAI_FS_FTD.csv")
#write.csv(FS_FTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_FS_FTD.csv \n", file = logfile, append = TRUE)


mu<-'select * from DISB_FTD'
DISB_FTD <- DBI::dbGetQuery(myconn3,mu)
# fn<-paste0("MUMBAI_DISB_FTD.csv")
# write.csv(DISB_FTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_DISB_FTD.csv \n", file = logfile, append = TRUE)



mu<-'select * from IS_FTD'
IS_FTD <- DBI::dbGetQuery(myconn3,mu)
# fn<-paste0("Mumbai_IS_FTD.csv")
# write.csv(IS_FTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_IS_FTD.csv \n", file = logfile, append = TRUE)


mu<-'select * from FS_YTD'
FS_YTD <- DBI::dbGetQuery(myconn3,mu)
# fn<-paste0("MUMBAI_FS_YTD.csv")
# write.csv(FS_YTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_FS_YTD.csv \n", file = logfile, append = TRUE)


mu<-'select * from DISB_YTD'
DISB_YTD <- DBI::dbGetQuery(myconn3,mu)
# fn<-paste0("MUMBAI_DISB_YTD.csv")
# write.csv(DISB_YTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_DISB_YTD.csv \n", file = logfile, append = TRUE)


mu<-'select * from IS_YTD'
IS_YTD <- DBI::dbGetQuery(myconn3,mu)
# fn<-paste0("MUMBAI_IS_YTD.csv")
# write.csv(IS_YTD,fn)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t MUMBAI_IS_YTD.csv \n", file = logfile, append = TRUE)


#----------------------------------Stage Wise Summary--------------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Stage wise Summary \n", file = logfile, append = TRUE)


q<-'SELECT distinct * from VSJ7'
mydata <- DBI::dbGetQuery(myconn3,q)
FINBRANCH<-sort(unique(mydata$FINBRANCH))
w<-data.frame(FINBRANCH)
p<-'select * from DS_STATUS_QUEUE_MAPPING'
p <- DBI::dbGetQuery(myconn3,p)


#Credit Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Credit Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Credit')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
cds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Credit')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(FINALLOANAMOUNT/10000000))

cds<-merge(x=cds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
cds<-replace(cds,is.na(cds),0)
g1<-cds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-cds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gcredit<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gcredit$Qhvol<-gcredit$Query.x + gcredit$Hold.x
gcredit$Qhval<-gcredit$Query.y + gcredit$Hold.y
gcredit<-gcredit[,c(1,12,26,4,18,5,19,30,31,10,24,3,17,7,21,6,20,11,25,2,16,9,23,15,29,14,28)]

#Ops Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Ops Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Ops')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
opsds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Ops')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(REQLOANAMT/1000000000))
opsds<-merge(x=opsds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
opsds<-replace(opsds,is.na(opsds),0)
g1<-opsds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-opsds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gops<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gops<-gops[,c(1,4,10,6,12,5,11,3,9,2,8,7,13)]


# Disbursement Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Disb Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Disbursement Queue')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
dds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Disbursement Queue')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(FINALLOANAMOUNT/10000000))
dds<-merge(x=dds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
dds<-replace(dds,is.na(dds),0)
g1<-dds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-dds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gdisb<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gdisb$BLvol<-gdisb$`LMS Activity.x` + gdisb$Booked.x
gdisb$BLval<-gdisb$`LMS Activity.y` + gdisb$Booked.y
gdisb<-gdisb[,c(1,3,8,4,9,12,13,6,11)]


#------------------------------Query Ageing for Stagewise---------------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Query Ageing for Stagewise - line 470 \n", file = logfile, append = TRUE)



q<-'SELECT distinct * from VSJ7'
calendar <- 'select * from holidaycalendar'
mydata <- DBI::dbGetQuery(myconn3,q)
h <- DBI::dbGetQuery(myconn3,calendar)



library(lubridate)
library(anytime)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t loading library lubridate \n", file = logfile, append = TRUE)


offsNumber<-Vectorize(function(close,open){
  if(is.null(close) | is.null(open) | is.na(close) | is.na(open)){
    return(0)
  }
  else if(as.numeric(as.Date(close)-as.Date(open))<=1){
    return (0)
  }
  else{
    day_seq <- seq(as.Date(open)+days(1), as.Date(close)-days(1), by = "days")
    #day_seq<-format(as.POSIXct(day_seq,format='%Y/%m/%d %H:%M:%S'),format='%Y-%m-%d')
    return(sum(as.character(day_seq) %in% as.character(holidays)))
  }
})

op<-data.frame()

offs<-h$DATES[h$BRANCH %in% c('All Branches','Common Holidays')]
holidays <- as.POSIXct(strptime(offs,"%d-%m-%Y"), tz='UTC')
holidays<-format(as.POSIXct(holidays,format='%Y-%m-%d'),format='%Y-%m-%d')
holidays<-unique(holidays)

isAge<-anytime(Sys.time(),asUTC=T)
attr(isAge, "tzone") <- "UTC" 
mydata$QUERY_AGEING_DAYS<-as.numeric(difftime(isAge,mydata$DATE_AND_TIME_RAISED_BY,
                                              units = "days"))
mydata$OffsQueryAgeing<-offsNumber(isAge,mydata$DATE_AND_TIME_RAISED_BY)
mydata$QueryAgeingFinal<-mydata$QUERY_AGEING_DAYS - mydata$OffsQueryAgeing
# mydata$QueryBands<-cut(mydata$QueryAgeingFinal,breaks = c(0,2,4,6,8,10,
#                                                           12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,
#                                                           54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,
#                                                           94,96,98,100))

mydata$QueryBands<-cut(mydata$QueryAgeingFinal,breaks = 0:100)

#----------------------------------Ops Queue HL Query Ageing-------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Ops Queue HL Query Ageing \n", file = logfile, append = TRUE)


FINBRANCH<-sort(unique(mydata$FINBRANCH))
b<-data.frame(FINBRANCH)
OpsQueryHL<-subset(mydata,mydata$QUEUE == 'Ops' & mydata$FINAL_DETAILED_STATUS == 'Query'
                   & !mydata$FINTYPE %in% c('LP','NP') )
OpsQueryHL<-ddply(OpsQueryHL,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
OpsQueryHL<-spread(OpsQueryHL,QueryBands,Count)
OpsQueryHL<-replace(OpsQueryHL,is.na(OpsQueryHL),0)
colnames(OpsQueryHL)<-gsub("[()]","",colnames(OpsQueryHL))
colnames(OpsQueryHL)<-gsub(",","-",colnames(OpsQueryHL))
colnames(OpsQueryHL)<-gsub("]","",colnames(OpsQueryHL))
OpsQueryHL<-merge(x = b,y = OpsQueryHL,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                  all.x = TRUE)
OpsQueryHL<-replace(OpsQueryHL,is.na(OpsQueryHL),0)

OpsQueryHL<- OpsQueryHL %>%
  adorn_totals("row")
OpsQueryHL<- OpsQueryHL %>%
  adorn_totals("col")


#--------------------------------Credit Queue HL Query Ageing------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Credit Queue HL Query Ageing \n", file = logfile, append = TRUE)

CreditQueryHL<-subset(mydata,mydata$QUEUE == 'Credit' & mydata$FINAL_DETAILED_STATUS == 'Query'
                      & !mydata$FINTYPE %in% c('LP','NP') )
CreditQueryHL<-ddply(CreditQueryHL,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
CreditQueryHL<-spread(CreditQueryHL,QueryBands,Count)
CreditQueryHL<-replace(CreditQueryHL,is.na(CreditQueryHL),0)
colnames(CreditQueryHL)<-gsub("[()]","",colnames(CreditQueryHL))
colnames(CreditQueryHL)<-gsub(",","-",colnames(CreditQueryHL))
colnames(CreditQueryHL)<-gsub("]","",colnames(CreditQueryHL))

CreditQueryHL<-merge(x = b,y = CreditQueryHL,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                     all.x = TRUE)
CreditQueryHL<-replace(CreditQueryHL,is.na(CreditQueryHL),0)

CreditQueryHL<- CreditQueryHL %>%
  adorn_totals("row")
CreditQueryHL<- CreditQueryHL %>%
  adorn_totals("col")

#-----------------------------LAP NRP Credit queue query Ageing-------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP NRP Queue Query Ageing \n", file = logfile, append = TRUE)

CreditQueryLAPNRP<-subset(mydata,mydata$QUEUE == 'Credit' & mydata$FINAL_DETAILED_STATUS == 'Query'
                          & mydata$FINTYPE %in% c('LP','NP') )
CreditQueryLAPNRP<-ddply(CreditQueryLAPNRP,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
CreditQueryLAPNRP<-spread(CreditQueryLAPNRP,QueryBands,Count)
CreditQueryLAPNRP<-replace(CreditQueryLAPNRP,is.na(CreditQueryLAPNRP),0)
colnames(CreditQueryLAPNRP)<-gsub("[()]","",colnames(CreditQueryLAPNRP))
colnames(CreditQueryLAPNRP)<-gsub(",","-",colnames(CreditQueryLAPNRP))
colnames(CreditQueryLAPNRP)<-gsub("]","",colnames(CreditQueryLAPNRP))
CreditQueryLAPNRP<-merge(x = b,y = CreditQueryLAPNRP,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                         all.x = TRUE)
CreditQueryLAPNRP<-replace(CreditQueryLAPNRP,is.na(CreditQueryLAPNRP),0)

CreditQueryLAPNRP<- CreditQueryLAPNRP %>%
  adorn_totals("row")
CreditQueryLAPNRP<- CreditQueryLAPNRP %>%
  adorn_totals("col")

#-----------------------------LAP NRP Ops queue query Ageing------------- 
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP NRP Ops queue query Ageing line 587 \n", file = logfile, append = TRUE)

tryCatch(			
  
  # Specifying expression
  expr = {
    OpsQueryLAPNRP<-subset(mydata,mydata$QUEUE == 'Ops' & mydata$FINAL_DETAILED_STATUS == 'Query'
                           & mydata$FINTYPE %in% c('LP','NP') )
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 592 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP<-ddply(OpsQueryLAPNRP,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 594 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP<-spread(OpsQueryLAPNRP,QueryBands,Count)
    
    
    #cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 597 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP<-replace(OpsQueryLAPNRP,is.na(OpsQueryLAPNRP),0)
    colnames(OpsQueryLAPNRP)<-gsub("[()]","",colnames(OpsQueryLAPNRP))
    colnames(OpsQueryLAPNRP)<-gsub(",","-",colnames(OpsQueryLAPNRP))
    colnames(OpsQueryLAPNRP)<-gsub("]","",colnames(OpsQueryLAPNRP))
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 601 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP<-merge(x = b,y = OpsQueryLAPNRP,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                          all.x = TRUE)
    OpsQueryLAPNRP<-replace(OpsQueryLAPNRP,is.na(OpsQueryLAPNRP),0)
    
    OpsQueryLAPNRP<- OpsQueryLAPNRP %>%
      adorn_totals("row")
    OpsQueryLAPNRP<- OpsQueryLAPNRP %>%
      adorn_totals("col")
    print("LAP NRP Ops queue query Ageing - Success")
  },
  # Specifying error message
  error = function(e){		
    print("There was an error message.")
  }
  
  #warning = function(w){	
  #  print("There was a warning message.")
  #},
  
)


#------------------------------LAP NRP Hold Ageing-----------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP NRP HOLD Ageing \n", file = logfile, append = TRUE)



q<-"SELECT DISTINCT A.*, B.LOGTIME
FROM VSJ4 A
left join (SELECT DISTINCT FINREFERENCE, MAX(LOGTIME) LOGTIME FROM ACTIVITY_LOG
       GROUP BY FINREFERENCE) B 
ON A.FINREFERENCE = B.FINREFERENCE
WHERE LOGTIME IS NOT NULL AND DETAILED_STATUS = 'Hold';"

ha <- DBI::dbGetQuery(myconn3,q)

ha$offsHoldAgeing<-offsNumber(isAge,ha$LOGTIME)
ha$HoldAgeing<-as.numeric(difftime(isAge,ha$LOGTIME,units='days'))
ha$HoldAgeingFinal<-ha$HoldAgeing - ha$offsHoldAgeing

# mydata$HoldBands<-cut(mydata$HoldAgeingFinal,breaks = c(0,2,4,6,8,10,
#                                                         12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,
#                                                         54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,
#                                                         94,96,98,100))
ha$HoldBands<-cut(ha$HoldAgeingFinal,breaks = 0:100)

HoldAgeingView<-subset(ha,ha$FINTYPE %in% c('LP') )
QLP<-HoldAgeingView
QLP<-QLP[,c('FINREFERENCE','FINBRANCH','DETAILED_STATUS','QUEUE','FINTYPE',
            'LOGIN_YEAR_MONTH','LOGTIME','HoldAgeingFinal')]
write.csv(QLP,'HoldAgeingDump.csv',row.names = FALSE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t HoldAgeingDump.csv \n", file = logfile, append = TRUE)


HoldAgeingView<-ddply(HoldAgeingView,c('FINBRANCH','HoldBands'),summarise,Count = length(FINREFERENCE))
HoldAgeingView<-spread(HoldAgeingView,HoldBands,Count)
HoldAgeingView<-replace(HoldAgeingView,is.na(HoldAgeingView),0)
colnames(HoldAgeingView)<-gsub("[()]","",colnames(HoldAgeingView))
colnames(HoldAgeingView)<-gsub(",","-",colnames(HoldAgeingView))
colnames(HoldAgeingView)<-gsub("]","",colnames(HoldAgeingView))
HoldAgeingView<-merge(x = b,y = HoldAgeingView,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                      all.x = TRUE)
HoldAgeingView<-replace(HoldAgeingView,is.na(HoldAgeingView),0)

HoldAgeingView<- HoldAgeingView %>%
  adorn_totals("row")
HoldAgeingView<- HoldAgeingView %>%
  adorn_totals("col")


#----------------------- Writing Stagewise Dashboard-----------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t  Preparing Excel -  Stagewise Dashboard \n", file = logfile, append = TRUE)


writeWorksheet(wb1,sheet = 'Summary',gops,startCol = 2,
               startRow = 19,header = FALSE)
writeWorksheet(wb1,sheet = 'Summary',gcredit,startCol = 2,
               startRow = 30,header = FALSE)
writeWorksheet(wb1,sheet = 'Summary',gdisb,startCol = 2,
               startRow = 41,header = FALSE)
writeWorksheet(wb1,sheet = 'Ageing',CreditQueryHL,startCol = 2,
               startRow = 5)
writeWorksheet(wb1,sheet = 'Ageing',OpsQueryHL,startCol = 2,
               startRow = 20)
writeWorksheet(wb1,sheet = 'Ageing',CreditQueryLAPNRP,startCol = 2,
               startRow = 36)
writeWorksheet(wb1,sheet = 'Ageing',OpsQueryLAPNRP,startCol = 2,
               startRow = 49)
writeWorksheet(wb1,sheet = 'Ageing',HoldAgeingView,startCol = 2,
               startRow = 65)

setForceFormulaRecalculation(wb1, sheet = '*', TRUE)

#------------------------------Saving LOS STAGEWISE Dashboard--------------------------

saveWorkbook(wb1,"LOS_STAGEWISE_SUMMARY.xlsx")
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t  LOS_STAGEWISE_SUMMARY.xlsx \n", file = logfile, append = TRUE)



#----------------------------------LAP Stage Wise Summary--------------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP Stage wise Summary \n", file = logfile, append = TRUE)


q<-"SELECT distinct * from VSJ7 where FINTYPE = 'LP'"
mydata <- DBI::dbGetQuery(myconn3,q)
FINBRANCH<-sort(unique(mydata$FINBRANCH))
w<-data.frame(FINBRANCH)
p<-'select * from DS_STATUS_QUEUE_MAPPING'
p <- DBI::dbGetQuery(myconn3,p)


#LAP Credit Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP Credit Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Credit')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
cds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Credit')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(FINALLOANAMOUNT/10000000))

cds<-merge(x=cds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
cds<-replace(cds,is.na(cds),0)
g1<-cds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-cds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gcredit<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gcredit$Qhvol<-gcredit$Query.x + gcredit$Hold.x
gcredit$Qhval<-gcredit$Query.y + gcredit$Hold.y
gcredit<-gcredit[,c(1,12,26,4,18,5,19,30,31,10,24,3,17,7,21,6,20,11,25,2,16,9,23,15,29,14,28)]

#Ops Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP Ops Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Ops')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
opsds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Ops')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(REQLOANAMT/1000000000))
opsds<-merge(x=opsds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
opsds<-replace(opsds,is.na(opsds),0)
g1<-opsds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-opsds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gops<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gops<-gops[,c(1,4,10,6,12,5,11,3,9,2,8,7,13)]


# LAP Disbursement Queue
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP Disb Queue \n", file = logfile, append = TRUE)


p1<-subset(p,p$QUEUE == 'Disbursement Queue')
DETAILED_STATUS<-p1[,c(1)]
v<-data.frame(DETAILED_STATUS)
dds<-merge(w,v,all = TRUE)

dd<-subset(mydata,mydata$QUEUE=='Disbursement Queue')
dd<-ddply(dd,c('FINBRANCH','FINAL_DETAILED_STATUS'),summarise,
          Count = length(FINREFERENCE),Value = sum(FINALLOANAMOUNT/10000000))
dds<-merge(x=dds,y=dd,by.x = c('FINBRANCH','DETAILED_STATUS'),by.y = c('FINBRANCH','FINAL_DETAILED_STATUS'),all.x = TRUE)
dds<-replace(dds,is.na(dds),0)
g1<-dds[,c(1,2,3)]
g1<-spread(g1,DETAILED_STATUS,Count)
g2<-dds[,c(1,2,4)]
g2<-spread(g2,DETAILED_STATUS,Value)
gdisb<-merge(x= g1,y=g2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',all.x = TRUE)
gdisb$BLvol<-gdisb$`LMS Activity.x` + gdisb$Booked.x
gdisb$BLval<-gdisb$`LMS Activity.y` + gdisb$Booked.y
gdisb<-gdisb[,c(1,3,8,4,9,12,13,6,11)]


#------------------------------Query Ageing for LAP Stagewise---------------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing Query Ageing for LAP Stagewise - line 470 \n", file = logfile, append = TRUE)



q<-"SELECT distinct * from VSJ7 where FINTYPE = 'LP'"
calendar <- 'select * from holidaycalendar'
mydata <- DBI::dbGetQuery(myconn3,q)
h <- DBI::dbGetQuery(myconn3,calendar)



library(lubridate)
library(anytime)

cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t loading library lubridate \n", file = logfile, append = TRUE)


offsNumber<-Vectorize(function(close,open){
  if(is.null(close) | is.null(open) | is.na(close) | is.na(open)){
    return(0)
  }
  else if(as.numeric(as.Date(close)-as.Date(open))<=1){
    return (0)
  }
  else{
    day_seq <- seq(as.Date(open)+days(1), as.Date(close)-days(1), by = "days")
    #day_seq<-format(as.POSIXct(day_seq,format='%Y/%m/%d %H:%M:%S'),format='%Y-%m-%d')
    return(sum(as.character(day_seq) %in% as.character(holidays)))
  }
})

op<-data.frame()

offs<-h$DATES[h$BRANCH %in% c('All Branches','Common Holidays')]
holidays <- as.POSIXct(strptime(offs,"%d-%m-%Y"), tz='UTC')
holidays<-format(as.POSIXct(holidays,format='%Y-%m-%d'),format='%Y-%m-%d')
holidays<-unique(holidays)

isAge<-anytime(Sys.time(),asUTC=T)
attr(isAge, "tzone") <- "UTC" 
mydata$QUERY_AGEING_DAYS<-as.numeric(difftime(isAge,mydata$DATE_AND_TIME_RAISED_BY,
                                              units = "days"))
mydata$OffsQueryAgeing<-offsNumber(isAge,mydata$DATE_AND_TIME_RAISED_BY)
mydata$QueryAgeingFinal<-mydata$QUERY_AGEING_DAYS - mydata$OffsQueryAgeing
# mydata$QueryBands<-cut(mydata$QueryAgeingFinal,breaks = c(0,2,4,6,8,10,
#                                                           12,14,16,18,20,22,24,26,28,30,32,34,36,38,40,42,44,46,48,50,52,
#                                                           54,56,58,60,62,64,66,68,70,72,74,76,78,80,82,84,86,88,90,92,
#                                                           94,96,98,100))

mydata$QueryBands<-cut(mydata$QueryAgeingFinal,breaks = 0:100)

#-----------------------------LAP Credit queue query Ageing-------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP Queue Query Ageing \n", file = logfile, append = TRUE)

CreditQueryLAPNRP2<-subset(mydata,mydata$QUEUE == 'Credit' & mydata$FINAL_DETAILED_STATUS == 'Query'
                           & mydata$FINTYPE %in% c('LP') )
CreditQueryLAPNRP2<-ddply(CreditQueryLAPNRP2,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
CreditQueryLAPNRP2<-spread(CreditQueryLAPNRP2,QueryBands,Count)
CreditQueryLAPNRP2<-replace(CreditQueryLAPNRP2,is.na(CreditQueryLAPNRP2),0)
colnames(CreditQueryLAPNRP2)<-gsub("[()]","",colnames(CreditQueryLAPNRP2))
colnames(CreditQueryLAPNRP2)<-gsub(",","-",colnames(CreditQueryLAPNRP2))
colnames(CreditQueryLAPNRP2)<-gsub("]","",colnames(CreditQueryLAPNRP2))
CreditQueryLAPNRP2<-merge(x = b,y = CreditQueryLAPNRP2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                          all.x = TRUE)
CreditQueryLAPNRP2<-replace(CreditQueryLAPNRP2,is.na(CreditQueryLAPNRP2),0)

CreditQueryLAPNRP2<- CreditQueryLAPNRP2 %>%
  adorn_totals("row")
CreditQueryLAPNRP2<- CreditQueryLAPNRP2 %>%
  adorn_totals("col")

#-----------------------------LAP Ops queue query Ageing------------- 
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Preparing LAP NRP Ops queue query Ageing line 587 \n", file = logfile, append = TRUE)

tryCatch(			
  
  # Specifying expression
  expr = {
    OpsQueryLAPNRP2<-subset(mydata,mydata$QUEUE == 'Ops' & mydata$FINAL_DETAILED_STATUS == 'Query'
                            & mydata$FINTYPE %in% c('LP') )
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 592 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP2<-ddply(OpsQueryLAPNRP2,c('FINBRANCH','QueryBands'),summarise,Count = length(FINREFERENCE))
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 594 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP2<-spread(OpsQueryLAPNRP2,QueryBands,Count)
    
    
    #cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 597 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP2<-replace(OpsQueryLAPNRP2,is.na(OpsQueryLAPNRP2),0)
    colnames(OpsQueryLAPNRP2)<-gsub("[()]","",colnames(OpsQueryLAPNRP2))
    colnames(OpsQueryLAPNRP2)<-gsub(",","-",colnames(OpsQueryLAPNRP2))
    colnames(OpsQueryLAPNRP2)<-gsub("]","",colnames(OpsQueryLAPNRP2))
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Debugging - line 601 \n", file = logfile, append = TRUE)
    
    OpsQueryLAPNRP2<-merge(x = b,y = OpsQueryLAPNRP2,by.x = 'FINBRANCH',by.y = 'FINBRANCH',
                           all.x = TRUE)
    OpsQueryLAPNRP2<-replace(OpsQueryLAPNRP2,is.na(OpsQueryLAPNRP2),0)
    
    OpsQueryLAPNRP2<- OpsQueryLAPNRP2 %>%
      adorn_totals("row")
    OpsQueryLAPNRP2<- OpsQueryLAPNRP2 %>%
      adorn_totals("col")
    print("LAP NRP Ops queue query Ageing - Success")
  },
  # Specifying error message
  error = function(e){		
    print("There was an error message.")
  }
  
  #warning = function(w){	
  #  print("There was a warning message.")
  #},
  
)



#----------------------- Writing LAP Stagewise Dashboard-----------------------
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t  Preparing Excel -  Stagewise Dashboard \n", file = logfile, append = TRUE)


writeWorksheet(wb3,sheet = 'Summary',gops,startCol = 2,
               startRow = 19,header = FALSE)
writeWorksheet(wb3,sheet = 'Summary',gcredit,startCol = 2,
               startRow = 30,header = FALSE)
writeWorksheet(wb3,sheet = 'Summary',gdisb,startCol = 2,
               startRow = 41,header = FALSE)
writeWorksheet(wb3,sheet = 'Ageing',CreditQueryLAPNRP2,startCol = 2,
               startRow = 5)
writeWorksheet(wb3,sheet = 'Ageing',OpsQueryLAPNRP2,startCol = 2,
               startRow = 20)


setForceFormulaRecalculation(wb3, sheet = '*', TRUE)

#------------------------------Saving LOS LAP Stagewise  Dashboard--------------------------

saveWorkbook(wb3,"LOS_STAGEWISE_SUMMARY_LAP.xlsx")
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t  LOS_STAGEWISE_SUMMARY_LAP.xlsx \n", file = logfile, append = TRUE)



#------------------------------VSJ7---------------------------

q<-"SELECT distinct * from VSJ7"
mydata <- DBI::dbGetQuery(myconn3,q)
write.csv(mydata,'VSJ7.csv')
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t VSJ7.csv \n", file = logfile, append = TRUE)


# mydata1<-subset(mydata,mydata$QUEUE=="Credit" & mydata$FINAL_DETAILED_STATUS
#                 == 'Query')
# mydata1<-mydata1[, c("FINREFERENCE","FINBRANCH","QUEUE","FINTYPE","LOGIN_YEAR_MONTH",
#                      "QUERY_STATUS","QueryAgeingFinal")]
# write.csv(mydata1,'VSJ7Urvi.csv',row.names = FALSE)
# cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t VSJ7Urvi.csv \n", file = logfile, append = TRUE)


#-------------------------DAILY DISB MIS (MTD)----------------------------
#     q<-"SELECT A.DISBDATE AS LOAN_START_DATE, A.FINREFERENCE as LANID, E.CUSTOMER_NAME, (B.PROJECTNAME ||', '|| B.CITY) PROJECTNAME, C.PROPERTY_ADDRESS, 
# (A.DISBAMOUNT/100) AS AMOUNT, A.DISBDATE AS DATE, D.VASREFERENCE,  --CUSTOMER_NAME,
# CASE WHEN A.DISBSEQ =1 THEN 'FIRST TRANCHE'
# ELSE 'FURTHER TRANCHE'
# END AS DISB_REMARKS
# FROM FINDISBURSEMENTDETAILS A 
# LEFT JOIN DTCRON B
# ON A.FINREFERENCE = B.FINREFERENCE
# LEFT JOIN
# (
#  SELECT DISTINCT REFERENCE, TOWER || ', ' || FLOORNUMBER || ', ' || UNIT AS PROPERTY_ADDRESS
#  FROM COLLATERAL_DATA 
# )C ON A.FINREFERENCE = C.REFERENCE
# LEFT JOIN 
# (
#  SELECT DISTINCT LANID, LISTAGG(VASREFERECE,', ') AS VASREFERENCE
#  FROM mis_insurance_v
#  GROUP BY LANID
# ) D ON A.FINREFERENCE = D.LANID
# LEFT JOIN MIS_CREDIT_LOAN_STATUS_VIEW3 E
# ON A.FINREFERENCE = E.LANID
# WHERE (MONTH(DISBDATE) = MONTH(CURRENT_DATE-4)) AND (YEAR(DISBDATE) = YEAR(CURRENT_DATE-4))
# --DISB_YEAR_MONTH = '2022-3'
# ORDER BY LOAN_START_DATE DESC;"
# 
# mydata <- DBI::dbGetQuery(myconn3,q)
#fn<-paste0("DAILY_DISB_DUMP",".csv")
#write.csv(mydata,"Daily_Disbursement_MIS.csv", row.names = FALSE)


#-----------------------------Detaching XLConnect and writing Openxlsx----------------------------
detach("package:XLConnect", unload = TRUE)
library(openxlsx)

dataset_names <- list('VSJ4'= VSJ4, 'FINDISB' = FINDISB)
write.xlsx(dataset_names, file = 'LOS_MakerChecker_Files.xlsx',overwrite = T)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_MakerChecker_Files.xlsx \n", file = logfile, append = TRUE)


dataset_names <- list('DISBQUEUE_SUMMARY' = DISBQUEUE_SUMMARY,
                      'CREDITQUEUE_SUMMARY' = CREDITQUEUE_SUMMARY,
                      'OPSQUEUE_SUMMARY' = OPSQUEUE_SUMMARY)
write.xlsx(dataset_names, file = 'LOS_Stagewise_Dump.xlsx',overwrite = T)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t LOS_Stagewise_Dump.xlsx \n", file = logfile, append = TRUE)


dataset_names <- list('FS_FTD' = FS_FTD,'DISB_FTD' = DISB_FTD,
                      'IS_FTD' = IS_FTD,
                      'FS_YTD' = FS_YTD,'DISB_YTD' = DISB_YTD,
                      'IS_YTD' = IS_YTD)
write.xlsx(dataset_names, file = 'Mumbai_Data.xlsx',overwrite = T)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t Mumbai_Data.xlsx \n", file = logfile, append = TRUE)


tryCatch(			
  
  # Specifying expression
  expr = {
    #------------------------------Dropping DTCRON Master Table---------------------------
    
    
    B <- "SET dtcron_Master=(SELECT 'DTCRON_MASTER' || '_' || TO_VARCHAR($today, 'DDMMYYYY'));"
    C<- "DROP TABLE IDENTIFIER($dtcron_Master);"
    DBI::dbGetQuery(myconn3,B)
    DBI::dbGetQuery(myconn3,C)
    print('Dropping DTcron Master Table & BAckup')
    
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DROPPED DTCRON MASTER \n", file = logfile, append = TRUE)
    
    B <- "SET vsj4_Master=(SELECT 'VSJ4_MASTER' || '_' || TO_VARCHAR($today, 'DDMMYYYY'));"
    C<- "DROP TABLE IDENTIFIER($vsj4_Master);"
    DBI::dbGetQuery(myconn3,B)
    DBI::dbGetQuery(myconn3,C)
    print('Dropping VSJ4 Master Table & BAckup')
    
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DROPPED DTCRON MASTER \n", file = logfile, append = TRUE)
    
  },
  error = function(e){		
    print("There was an error message.")
  },
  
  #warning = function(w){	
  #  print("There was a warning message.")
  #},
  finally = {			
    print("Executed - LOS backup tables")
  }
)


#-----------PREPARING DTcron_Master------------

tryCatch(			
  
  # Specifying expression
  expr = {
    #------------------------------DTCRON Master Table---------------------------
    print('Preparing DTcron Master Table & BAckup')
    e <- format(Sys.time(), "%d%m%Y")
    A <- "SET today = TO_DATE(CONVERT_TIMEZONE('Asia/Kolkata',CURRENT_TIMESTAMP()));"
    DBI::dbGetQuery(myconn3,A)
    B <- "SET DTCRON_MASTER=(SELECT 'DTCRON_MASTER' || '_' || TO_VARCHAR($today, 'DDMMYYYY'));"
    C <- "CREATE TABLE IDENTIFIER($DTCRON_MASTER) as select distinct * from DTCRON;"
    DBI::dbGetQuery(myconn3,B)
    mydata <- DBI::dbGetQuery(myconn3,C)
    fn<-paste0("DTCRON_Master.csv")
    write.csv(mydata,fn)
    
    
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t DTCRON backup created as DTCRON_MASTER_DDMMYYYY \n", file = logfile, append = TRUE)
    
    #------------------------------VSJ4 Master Table---------------------------
    
    B <- "SET VSJ4_Master=(SELECT 'VSJ4_MASTER' || '_' || TO_VARCHAR($today, 'DDMMYYYY'));"
    C<- "CREATE TABLE IDENTIFIER($VSJ4_Master) as select distinct * from VSJ4;"
    DBI::dbGetQuery(myconn3,B)
    mydata <- DBI::dbGetQuery(myconn3,C)
    fn<-paste0("VSJ4_Master.csv")
    write.csv(mydata,fn)
    
    
    cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t VSJ4 backup created as VSJ4_MASTER_DDMMYYYY \n", file = logfile, append = TRUE)
  },
  # Specifying error message
  error = function(e){		
    print("There was an error message.")
  },
  
  #warning = function(w){	
  #  print("There was a warning message.")
  #},
  finally = {			
    print("Executed - LOS backup tables")
  }
)


cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t CODE ENDS ---------- \n", file = logfile, append = TRUE)
cat(format(Sys.time(), "%d-%m-%Y %H.%M"),"\t CODE ENDS ---------- \n")

