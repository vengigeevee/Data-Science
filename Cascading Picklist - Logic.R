rm(list=ls())
library(data.table)
library(dplyr)
SR <- fread("SR_data.csv")
pick <- fread("pick.csv")

df1 <- data.table(parent=as.character(),child=as.character(),parent_val=as.character(),child_val=as.character())

for(i in 1:nrow(pick)) {
v1 <- as.character(pick[i])

df <- data.table(cbind(Parent=v1[1],child=v1[2],SR[,v1,with=FALSE]))
names(df) <- names(df1)

df1 <- union(df1,df)
}
