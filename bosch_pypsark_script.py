#%% Adatok generálása DataFrame-be

import os
import random
import pandas as pd
from datetime import datetime, timedelta


# Alapbeállítások
# ==========================
N_ROWS = 1000000  # teszteléshez; később 1000000-ra állítjuk
OUTPUT_DIR = "data/raw"
OUTPUT_FILE = "sensor_data_test.csv"

# Mappa létrehozása, ha nem létezik
os.makedirs(OUTPUT_DIR, exist_ok=True)

# Paraméter tartományok
PARAMETER_RANGES = {
    "Temperature": (-20, 120),
    "Noise": (0, 150),
    "Offset": (-5, 5),
    "Vibration": (0, 10),
}

# Segédfüggvények
# ==========================
def random_sensor_id():
    """Véletlenszerű 7 karakteres azonosító betűkből és számokból."""
    chars = "abcdefghijklmnopqrstuvwxyz0123456789"
    return "".join(random.choices(chars, k=7))

def random_date(start_date, end_date):
    """Véletlen dátum két időpont között (nap pontossággal)."""
    delta = end_date - start_date
    random_days = random.randint(0, delta.days)
    return (start_date + timedelta(days=random_days)).strftime("%Y-%m-%d")

def generate_value(parameter, status):
    """Konzisztens érték generálása a paraméter és státusz alapján."""
    low, high = PARAMETER_RANGES[parameter]
    value = random.uniform(low, high)
    # Ha 'Bad' a státusz, dobjunk be néha outlier értéket
    if status == "Bad":
        # pl. 10%-ban extrém érték
        if random.random() < 0.1:
            value *= random.choice([-3, 3])
    return round(value, 2)

def generate_sensor_data(n_rows: int):
    locations = ["Hungary", "Germany", "UK", "China", "Mexico"]
    parameters = list(PARAMETER_RANGES.keys())
    statuses = ["Good", "Bad"]

    start_date = datetime(2025, 1, 1)
    end_date = datetime(2025, 10, 28)

    data = []
    for _ in range(n_rows):
        status = random.choices(statuses, weights=[0.9, 0.1])[0]
        parameter = random.choice(parameters)
        value = generate_value(parameter, status)

        row = {
            "Sensor_ID": random_sensor_id(),
            "Date": random_date(start_date, end_date),
            "Location": random.choice(locations),
            "Parameter": parameter,
            "Value": value,
            "Status": status,
        }
        data.append(row)

    return pd.DataFrame(data)

# ==========================
# Fő futtatás
# ==========================
if __name__ == "__main__":
    df = generate_sensor_data(N_ROWS)
    output_path = os.path.join(OUTPUT_DIR, OUTPUT_FILE)
    df.to_csv(output_path, index=False)
    print(f"{len(df)} sor generálva -> {output_path}")
    print(df.head())
    
    
#%%  PySpark környezet inicializálás , és adatbeolvasás

import os
from pyspark.sql import SparkSession
from pyspark.sql import functions as F
from pyspark.sql.window import Window

# Dummy hadoop beállítás a hibeüzenet elkerülésére
os.environ["JAVA_HOME"] = r"C:\Program Files\Eclipse Adoptium\jdk-17.0.16.8-hotspot"
os.environ["HADOOP_HOME"] = r"C:\hadoop"
#os.makedirs("C:\\hadoop\\bin", exist_ok=True)

# Spark inicializálás
# ==========================
spark = (
    SparkSession.builder
    .appName("BoschSparkTest")
    .master("local[*]")
    .getOrCreate()
)

print("SparkSession létrejött:", spark.version)


# Adatbeolvasás
# ==========================
input_path = "data/raw/sensor_data_test.csv"

df = (
    spark.read.option("header", True)
    .option("inferSchema", True)
    .csv(input_path)
)


df.printSchema()
df.show(5)

#%% SPark Transzformációk

# Alap transzformációk
# ==========================

# Átlagos érték paraméteren és státuszonként
agg_df = (
    df.groupBy("Parameter", "Status")
      .agg(
          F.count("*").alias("count_rows"),
          F.avg("Value").alias("avg_value"),
          F.stddev("Value").alias("stddev_value")
      )
      .orderBy("Parameter", "Status")
)

print("📊 Aggregált statisztika:")
agg_df.show()

# Window példaként: hogy adott szenzoron belül hanyadik minta
window_spec = Window.partitionBy("Sensor_ID").orderBy("Date")
df_with_rank = df.withColumn("row_num", F.row_number().over(window_spec))
print("Példa Window függvényre:")
df_with_rank.select("Sensor_ID", "Date", "Parameter", "Value", "row_num").show(5)

# Napi átlag paraméterenként
pivot_df = (
    df.groupBy("Date", "Location")
      .pivot("Parameter")
      .agg(F.avg("Value"))
      .orderBy("Date")
)

print("📈 Pivot DataFrame:")
pivot_df.show(5)

#%% 
# Mentés Parquet-be
# ==========================
output_dir = "data/processed"
os.makedirs(output_dir, exist_ok=True)
output_path = os.path.join(output_dir, "sensor_processed.parquet")

pivot_df.write.mode("overwrite").parquet(output_path)
print(f"mentve Parquet formátumban ide: {output_path}")


# Lezárás
# ==========================
spark.stop()
print("🧹 SparkSession leállítva.")