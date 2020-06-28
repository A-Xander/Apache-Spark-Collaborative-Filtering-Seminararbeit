# Laden der benötigten Bibliotheken in die R Session
# Gegebenfalls Bibliotheken vorher installieren
library(sparklyr)
library(caTools)

# Lokale Spark Verbindung herstellen
sc <- spark_connect(master = "local")

# Mittlere quadratische Abweichung Formel
RMSE = function(m, o){
  sqrt(mean((m - o)^2))
}

# Datensatz als R Dataframe abspeichern
# Gegebenfalls Pfade anpassen
book_ratings_df <- read.delim("D:/Downloads/bookdata/book_ratings.dat") # Bewertungsmatrix
items_info_df <- read.csv("D:/Downloads/bookdata/items_info.csv", TRUE,";") # Informationen
names(items_info_df)[1] <- "item"
users_info_df <- read.csv("D:/Downloads/bookdata/users_info.csv", TRUE,";") # Informationen
names(users_info_df)[1] <- "user"

# Seed setzen, damit die gleiche Aufteilung wieder verwendet werden kann
set.seed(101) 

# Aufteilung in Training- und Testdaten
sample = sample.split(book_ratings_df$user, SplitRatio = .80)
train_df = subset(book_ratings_df, sample == TRUE)
test_df  = subset(book_ratings_df, sample == FALSE)

# Datensatz als Spark Tabelle in R wrappen
train_tbl <- sdf_copy_to(sc, train_df)
test_tbl <- sdf_copy_to(sc, test_df)

# Ausfuehren des ALS Algorithmus mit den Trainingsdaten
model <- ml_als(train_tbl, rating ~ user + item)

# Vorhersage für Testdaten anhand des Modelles
predictions_df <- data.frame(ml_predict(model, test_tbl))

# Anzahl NaN Vorhersagen
nan_count <- sum(predictions_df$prediction == "NaN")

# Entfernung der NaN Vorhersage Zeilen
predictions_df <- predictions_df[predictions_df$prediction != "NaN", ]

# Mittlere quadratische Abweichung
# 3. Spalte entspricht wahrer Bewertung, 6. Spalte entspricht Vorhersagewert
rmse <- RMSE(predictions_df[ ,3], predictions_df[ ,6])

# Einfuegen der Informationen aus dem Nutzer und Buecher Datensatz
predictions_info_df <- merge(predictions_df, users_info_df,  by.predictions_df = "user", by.users_info_df = 'user')
predictions_info_df <- merge(predictions_info_df, items_info_df,  predictions_info_df = "item", items_info_df = 'item')
