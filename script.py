from pyspark.sql import SparkSession
from pyspark.sql.functions import col,floor,when
from pymongo import MongoClient
spark = SparkSession.builder.appName("myApp").getOrCreate()

chemin_fichier_csv ="heart_data2.csv"
data_frame = spark.read.option("header", "true") .option("inferSchema", "true") .csv(chemin_fichier_csv)

data_frame.printSchema()

data_frame.show(10)

client = MongoClient("mongodb+srv://url_mongodb")
db = client["bigData"]
def create_profiles_collection(db, collection_name, validator=None):
    if collection_name not in db.list_collection_names():
        if validator:
            db.create_collection(collection_name, validator=validator)
        else:
            db.create_collection(collection_name)
    else:
        print(f"Collection '{collection_name}' already exists.")
        
def insert_data_to_mongo(db, collection_name, data_frame):
    collection = db[collection_name]
    records = data_frame.to_dict(orient='records')
    collection.insert_many(records)

create_profiles_collection(db, "heart_1")  # Create collection if it doesn't exist
insert_data_to_mongo(db, "heart_1", data_frame.toPandas())  # Insert data

data_frame.createOrReplaceTempView("cardio_table")
resultat_sql = spark.sql("SELECT * FROM cardio_table ")

resultat_sql.show(10)

def convert_days_to_years(age_in_days):
    return floor(age_in_days / 365.25).cast("int")

# Application de la fonction et remplacement direct de la colonne "age"
data_frame = data_frame.withColumn("age", convert_days_to_years(col("age")))

# Affichage du DataFrame modifié
data_frame.show(10)

data_frame = data_frame.withColumn("Categories_age", 
                                  when(col("age")<14, "Enfants")
                                  .when((col("age")>14) & (col("age")<30), "Adolescents")
                                  .when((col("age")>30) & (col("age")<60), "Adultes")
                                  .otherwise("Aînés")
                                 )

# Affichage du DataFrame modifié
data_frame.show(10)

resultat_with_cardio=data_frame.filter(col("cardio")=="YES")
total_cardio=resultat_with_cardio.count()
resultat=resultat_with_cardio.groupBy("gender","Categories_age").count()
rdd1=resultat.withColumn("pourcentage",(col("count")/total_cardio)*100)
rdd1.show()

create_profiles_collection(db, "rdd1")  # Create collection if it doesn't exist
insert_data_to_mongo(db, "rdd1", rdd1.toPandas())  # Insert data in mongoDB


resultat_smoke=resultat_with_cardio.groupBy("gender","Categories_age","smoke").count()
rdd2=resultat_smoke.withColumn("pourcentage",(col("count")/total_cardio)*100)
rdd2.show()

create_profiles_collection(db, "rdd2")  # Create collection if it doesn't exist
insert_data_to_mongo(db, "rdd2", rdd2.toPandas())  # Insert data in mongoDB


data_frame = data_frame.withColumn("gender", 
                                  when(col("gender") =="H", 1)
                                  .when(col("gender") =="F", 2)
                                  .otherwise(col("gender"))
                                 )

# Affichage du DataFrame modifié
data_frame.show(10)

def remplacer_genre_binaire(dataframe, colonne_genre):

  return dataframe.withColumn(colonne_genre, 
                              when(col(colonne_genre) == "YES", 1)
                              .when(col(colonne_genre) == "NO", 0)
                              .otherwise(col(colonne_genre))
                             )

data_frame=remplacer_genre_binaire(data_frame, "smoke")
data_frame=remplacer_genre_binaire(data_frame, "alco")
data_frame=remplacer_genre_binaire(data_frame, "active")
data_frame=remplacer_genre_binaire(data_frame, "cardio")

# Affichage du DataFrame modifié
data_frame.show(10)

def remplacer_genre(dataframe, colonne_genre):

  return dataframe.withColumn(colonne_genre, 
                              when(col(colonne_genre) == "normal",1 )
                              .when(col(colonne_genre) == "above normal",2 )
                               .when(col(colonne_genre) == "well above normal",3 )
                              .otherwise(col(colonne_genre))
                             )

data_frame=remplacer_genre(data_frame, "cholesterol")
data_frame=remplacer_genre(data_frame, "gluc")

# Affichage du DataFrame modifié
data_frame.show(10)

create_profiles_collection(db, "heart_2")  # Create collection if it doesn't exist
insert_data_to_mongo(db, "heart_2", data_frame.toPandas())  # Insert data in mongoDB

