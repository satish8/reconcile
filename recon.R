##accessing the database
library(dplyr)
library(tidyr)
library(readr)
library(RPostgreSQL)
library(sqldf)
library(DescTools)
library(mailR)

options(sqldf.driver = "SQLite") # as per FAQ #7 force SQLite
options(gsubfn.engine = "R") # as per FAQ #5 use R code rather than tcltk

#Get current date
current_date <- Sys.Date()
#current_date <- current_date - 3


##open connection to database
print("connecting to DB")
con_sb <- src_postgres(host = "giraffe.#########.rds.amazonaws.com", 
                       port = "5432", dbname = "production", 
                       user = "", 
                       password = "")



##load necessary tables from the database

print("loading tables from DB")
credit_orders <- tbl(con_sb,sql("select * (order_id,'9999999') > 0"))
credit_orders<- as.data.frame(credit_orders)

order_emis <- tbl(con_sb,sql("select * from(order_id,'9999999') > 0"))
order_emis<- as.data.frame(order_emis)


app_nbrs <- tbl(con_sb, sql("
                            from app_numbers where number_status = 'VERIFIED'"))
app_nbrs <- as.data.frame(app_nbrs)

rm(con_sb)

#Convert all date fields to R format, as otherwise stays in Unix Epoch format and causes issues later
credit_orders$created_at <- as.Date(credit_orders$created_at, tz='Asia/Kolkata')
credit_orders$updated_at <- as.Date(credit_orders$updated_at, tz='Asia/Kolkata')
credit_orders$lead_date <- as.Date(credit_orders$lead_date, tz='Asia/Kolkata')
credit_orders$agreement_date <- as.Date(credit_orders$agreement_date, tz='Asia/Kolkata')
credit_orders$product_order_date <- as.Date(credit_orders$product_order_date, tz='Asia/Kolkata')
credit_orders$product_delivered_date <- as.Date(credit_orders$product_delivered_date, tz='Asia/Kolkata')
credit_orders$return_refund_date_on_site <- as.Date(credit_orders$return_refund_date_on_site, tz='Asia/Kolkata')
credit_orders$rc_refund_date <- as.Date(credit_orders$rc_refund_date, tz='Asia/Kolkata')
credit_orders$last_payment_date <- as.Date(credit_orders$last_payment_date, tz='Asia/Kolkata')
credit_orders$next_payment_date <- as.Date(credit_orders$next_payment_date, tz='Asia/Kolkata')

order_emis$payment_date <- as.Date(order_emis$payment_date, tz='Asia/Kolkata')
order_emis$due_date <- as.Date(order_emis$due_date, tz='Asia/Kolkata')
order_emis$created_at <- as.Date(order_emis$created_at, tz='Asia/Kolkata')
order_emis$updated_at <- as.Date(order_emis$updated_at, tz='Asia/Kolkata')


#write_csv(credit_orders,path="credit_orders.csv")


order_emis_full <- merge(credit_orders,order_emis,by=c("order_id"))


order_emis$order_id <- as.numeric(order_emis$order_id)


#Issue with NA return type for groupby+mutate+ifelse as per https://github.com/hadley/dplyr/issues/1036
#So need to explictly convert to numeric

#Get aggregate emi    
calc_order_emis <- order_emis_full %>%  
  mutate(order_id = as.numeric(order_id)) %>%
  mutate(rc_balance_for_order = rc_balance_for_order.x) %>%
  mutate(credit_limit = credit_limit.x) %>%
  group_by(order_id) %>%
  mutate(calc_due_date = AddMonths(agreement_date,emi_number-1)) %>%
  mutate(calc_total_payment_made = sum(payment_amount, na.rm=TRUE)) %>%  
#  mutate(calc_amount_paid_by_due_date = sum(as.numeric(ifelse(payment_date <= calc_due_date,payment_amount,0)),na.rm=TRUE)) 
  mutate(calc_amount_paid_by_due_date = sum(as.numeric(ifelse(payment_date <= calc_due_date,payment_amount,0)),na.rm=TRUE)) %>%  
  mutate(calc_amount_paid_after_due_date = sum(as.numeric(ifelse(payment_date > calc_due_date,payment_amount,0)),na.rm=TRUE))  %>% 
  mutate(calc_last_payment_date = as.Date(max(ifelse(payment_amount > 0,payment_date,0),na.rm=TRUE),origin = "1970-01-01"))   %>% 
  mutate(temp_date = ifelse(emi_number > 1 & payment_amount == 0 & due_amount > 0, calc_due_date, 999999))  %>%
  mutate(calc_next_payment_date = as.Date(min(ifelse(emi_number > 1 & payment_amount == 0 & due_amount > 0, calc_due_date, 999999),na.rm=TRUE),origin = "1970-01-01")) %>%
  mutate(emi_n_today = max(as.numeric(ifelse(calc_due_date <= as.character.Date(current_date),emi_number,0))))  %>%
  mutate(emi_n_nextdate = min(as.numeric(ifelse(emi_number > 1 & payment_amount < due_amount & due_amount > 0,emi_number,999999)))) %>%
  mutate(emi_n_last_paid = max(as.numeric(ifelse(payment_amount > 0 ,emi_number,0)),na.rm=TRUE)) %>%
  mutate(calc_last_payment_actual_amount = max(as.numeric(ifelse(emi_number == emi_n_last_paid,payment_amount,0)),na.rm=TRUE)) %>%
  mutate(calc_next_payment_due_amount = max(0,as.numeric(ifelse(emi_number == emi_n_nextdate,due_amount,0)),na.rm=TRUE)) %>%
  ungroup()  %>%
#  mutate(order_id = as.numeric(order_id)) %>%
  mutate(product_type=ifelse(regexpr('^.*accident.*$',product_name, ignore.case = TRUE) > 0,"Accidental Order","Other"))  %>%
  mutate(product_type=ifelse(regexpr('^.*excess.*amount.*$',product_name, ignore.case = TRUE) > 0,"Excess Pymt",product_type))  %>%
  mutate(product_type=ifelse(regexpr('^.*wrong.*order.*$',product_name, ignore.case = TRUE) > 0,"Wrong Order",product_type))  %>%
  mutate(product_type=ifelse(regexpr('^.*late.*fine.*$',product_name, ignore.case = TRUE) > 0,"Late Fine",product_type))  %>%
  mutate(product_type=ifelse(regexpr('^.*dummy.*$',product_name, ignore.case = TRUE) > 0,"Dummy Order",product_type)) %>%
  mutate(product_type=ifelse(regexpr('^.*cash.*back.*$',product_name, ignore.case = TRUE) > 0,"Cashback",product_type)) %>%
  mutate(product_type=ifelse(regexpr('^.*recharge|reacharge.*$',product_name, ignore.case = TRUE) > 0,"Recharge",product_type)) %>%
  mutate(product_type=ifelse(regexpr('^.*bill.*payment.*$',product_name, ignore.case = TRUE) > 0,"Bill Payment",product_type)) %>%
  mutate(product_type=ifelse(regexpr('^.*movie.*$',product_name, ignore.case = TRUE) > 0,"Movie Tickets",product_type)) %>%
  mutate(product_type=ifelse(regexpr('^.*bus|train|flight.*$',product_name, ignore.case = TRUE) > 0,"Travel",product_type)) %>%
  mutate(emi_plan_mnths = ifelse(emi_plan == '30 day', 1,
                                 ifelse (emi_plan == '3 months', 3, 
                                         ifelse (emi_plan == '6 months', 6,0)
                                 )
  )) %>%
  mutate(downpayment_pct = 0.2) %>%
#  mutate(zero_downpayment_limit = 300) %>%
  
  mutate(interest_rate_tenure = ifelse(emi_plan_mnths == 1, 0, 0.02*emi_plan_mnths)) %>%
  mutate(calc_down_payment = 
           ifelse(emi_plan_mnths == 1, 
                  0,
                  pmax(round(pmin(product_price,
                                  avail_credit_limit_manual_before_order)*downpayment_pct/10+0.01,0)*10 + 
                         round(pmax(0,(product_price - 
                                         avail_credit_limit_manual_before_order))/10+0.01,0)*10, 0
                       
                  )))  %>%
  mutate(calc_monthly_emi = as.numeric(ifelse(emi_plan_mnths == 1, 
                                              product_price,
                                   ceiling(round((product_price-calc_down_payment)*(1+interest_rate_tenure),2)/(emi_plan_mnths*10))*10))
         
  ) 


write_csv(calc_order_summary,path="calc_order_summary.csv")
