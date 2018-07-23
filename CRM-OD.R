rm(list=ls())
gc(reset =TRUE)
t1 <- Sys.time()
library(data.table,warn.conflicts = FALSE)
library(stringr,warn.conflicts = FALSE)
library(dplyr,warn.conflicts = FALSE)
library(tidyr,warn.conflicts = FALSE)
library(tibble,warn.conflicts = FALSE)

in_path <- "D:/GM_OCOD/Account/source"
out_path <- "D:/GM_OCOD/Account/Output/"
val_path <- "D:/GM_OCOD/Account/Validation/"
conf <- fread("D:/GM_OCOD/Account/Conf/Account_conf.csv",na="")
batch_size <- 100000
req_cols <- unique(na.omit(conf$REQ_COL))
lov_cols <- unique(na.omit(conf$LOV_COL))
sel_cols <- unique(na.omit(conf$SEL_COL)) 
setwd(in_path)
#getwd()
file_name <-dir(in_path, pattern =".csv")


##############################################################################################

Req_Data_out <- vector('integer')
sel_data_out <- vector('integer')
lov_data_df  <- data.table(column = character(), Value = character())

for(i in 1:length(file_name)) {
  
  sel_data <- fread(file_name[i],select = sel_cols)  
  sel_data_out <- c(sel_data_out,colSums(!(sel_data=="" | is.na(sel_data) |sel_data==" "))) 
  
  Req_Data <- sel_data[,req_cols,with=FALSE]
  Req_Data_out <- c(Req_Data_out,colSums(Req_Data=="" | is.na(Req_Data)|Req_Data==" ")) 
  
  lov_Data <- sel_data[,lov_cols,with=FALSE]
  lov_Data_g <- gather(lov_Data,"column","Value") %>% filter(Value!="") %>% unique()
  lov_data_df <- unique(bind_rows(lov_data_df,lov_Data_g))
  
  
  min_rec <-1
  max_rec <-batch_size
  batch <- ceiling(nrow(sel_data)/batch_size)
  
  for (j in 1:batch) {
    file_nm <- paste(out_path,str_sub(file_name[i],1,-5),"_00",j,".csv",sep="")
    file_rng <- min_rec:min(max_rec,nrow(sel_data))
    fwrite(sel_data[file_rng],file_nm) 
    min_rec <- min_rec+batch_size
    max_rec <- max_rec+batch_size
  }
  
  rm(list=c("sel_data","Req_Data","lov_Data","lov_Data_g"))
  gc(reset =TRUE)
  
}

Req_Data_df <- data.frame(col_name=names(Req_Data_out),count =as.integer(Req_Data_out)) 
Req_Data_df <- Req_Data_df %>% group_by(col_name) %>% summarise(count=sum(count)) %>% filter(count>0) %>% arrange(desc(count))

sel_data_df <- data.frame(col_name=names(sel_data_out),count =as.integer(sel_data_out)) 
sel_data_df <- sel_data_df %>% group_by(col_name) %>% summarise(count=sum(count)) %>% arrange(count)


setwd(val_path)
fwrite(Req_Data_df,"Req_Data_Val.csv")
fwrite(sel_data_df,"Sel_Data_Val.csv")
fwrite(lov_data_df,"Lov_Data_Val.csv")

rm(lov_data_df)

t2 <- Sys.time()
t2-t1

.rs.restartR()
