# Laden der sparklyr Bibliothek in die R Session
library(sparklyr)

# Lokale Spark Verbindung herstellen
sc <- spark_connect(master = "local")

# Datensatz als R Dataframe abspeichern
example_df <- data.frame(
  user   = c(1, 2, 0, 1, 2, 0),
  item   = c(1, 1, 1, 2, 2, 0),
  rating = c(3, 1, 2, 4, 5, 4)
)

train_df <- example_df
test_df <- example_df

# Datensatz als Spark Tabelle in R wrappen
train_tbl <- sdf_copy_to(sc, train_df)
test_tbl <- sdf_copy_to(sc, test_df)

# Ausfuehren des ALS Algorithmus zum Trainieren eines Modelles
model <- ml_als(train_tbl, rating ~ user + item)

# Vorhersage anhand des Modelles
predictions_df <- data.frame(ml_predict(model, test_tbl))