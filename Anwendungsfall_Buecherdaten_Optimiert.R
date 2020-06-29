library(sparklyr)
library(caTools)

sc <- spark_connect(master = "local")

book_ratings_df <- read.delim("D:/Downloads/bookdata/book_ratings.dat")
items_info_df <- read.csv("D:/Downloads/bookdata/items_info.csv", TRUE,";")
names(items_info_df)[1] <- "item"
users_info_df <- read.csv("D:/Downloads/bookdata/users_info.csv", TRUE,";")
names(users_info_df)[1] <- "user"

set.seed(101) 

sample = sample.split(book_ratings_df$user, SplitRatio = 0.8)
train_df = subset(book_ratings_df, sample == TRUE)
test_df  = subset(book_ratings_df, sample == FALSE)

train_tbl <- sdf_copy_to(sc, train_df, overwrite = TRUE)
test_tbl <- sdf_copy_to(sc, test_df, overwrite = TRUE)

model <- ml_als(train_tbl, rating ~ user + item, rank = 3, reg_param = 0.54, max_iter = 10)
  
predictions_df <- data.frame(ml_predict(model, test_tbl))
  
nan_count <- sum(predictions_df$prediction == "NaN")
  
predictions_df <- predictions_df[predictions_df$prediction != "NaN", ]

ml_recommend(model, type = "item", 3)

predictions_info_df <- merge(predictions_df, users_info_df,  by.predictions_df = "user", by.users_info_df = 'user')
predictions_info_df <- merge(predictions_info_df, items_info_df,  predictions_info_df = "item", items_info_df = 'item')
