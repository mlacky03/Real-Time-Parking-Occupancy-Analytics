import os
import glob
import time
import pandas as pd
from google.cloud import bigquery

os.environ['GOOGLE_APPLICATION_CREDENTIALS'] = 'inzinjerstvolab4-6395b1531b01.json' 

PROJECT_ID = 'inzinjerstvolab4'          
DATASET_ID = 'parking_nis'
TABLE_SIROVI = f'{PROJECT_ID}.{DATASET_ID}.sirovi_eventi'
TABLE_ZONE = f'{PROJECT_ID}.{DATASET_ID}.zone_agregacije'
TABLE_VOZILA = f'{PROJECT_ID}.{DATASET_ID}.vozila_agregacije'
TABLE_PLACANJE = f'{PROJECT_ID}.{DATASET_ID}.placanje_agregacije'



FOLDER_SIROVI = 'izlaz/sirovi_eventi'
FOLDER_METRIKA1 = 'izlaz/metrika1_zona'
FOLDER_METRIKA2 = 'izlaz/metrika2_vozila'
FOLDER_METRIKA3 = 'izlaz/metrika3_placanje'
FOLDER_TRACK = 'izlaz/upisani_fajlovi'

INTERVAL_SEC = 60 


client = bigquery.Client()

def ucitaj_upisane() -> set:
    if not os.path.exists(FOLDER_TRACK):
        return set()
    with open(FOLDER_TRACK, "r") as f:
        return set(line.strip() for line in f if line.strip())
    
upisani_fajlovi = ucitaj_upisane()



def upisi_u_bigquery(df: pd.DataFrame, table_id: str):
    if df.empty:
        print(f"  Nema podataka za {table_id}, preskačem.")
        return
    job_config = bigquery.LoadJobConfig()
    job_config.write_disposition = bigquery.WriteDisposition.WRITE_APPEND
    job = client.load_table_from_dataframe(df, table_id, job_config=job_config)
    job.result()
    print(f"  ✓ Upisano {len(df)} redova → {table_id}")


def pripremi_sirove(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    if 'timestamp' in df.columns:
        df['timestamp'] = pd.to_datetime(df['timestamp'], errors='coerce')
    return df



def pripremi_timestamp_kolone(df: pd.DataFrame) -> pd.DataFrame:
    if df.empty:
        return df
    for col in ['prozor_start', 'prozor_end']:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors='coerce')
    return df




def novi_fajlovi(folder: str) -> list:
    svi = set(glob.glob(os.path.join(folder, "*.csv")))
    novi = svi - upisani_fajlovi
    return list(novi)

def oznaci_upisane(fajlovi: list):
    upisani_fajlovi.update(fajlovi)
    with open(FOLDER_TRACK, "a") as f:
        f.write('\n'.join(fajlovi) + '\n')




def main():
    print("=" * 55)
    print("  BigQuery Uploader — Parking Niš")
    print(f"  Projekat: {PROJECT_ID}")
    print(f"  Dataset:  {DATASET_ID}")
    print(f"  Interval: {INTERVAL_SEC}s")
    print("  Zaustavi sa Ctrl+C")
    print("=" * 55)
  

    iteracija = 0
    try:
        while True:
            iteracija += 1
            print(f"\n[Iteracija {iteracija}] Čitam nove CSV fajlove...")

            novi_s = novi_fajlovi(FOLDER_SIROVI)
            if novi_s:
                df_sirovi = pd.concat(
                    [pd.read_csv(f) for f in novi_s], ignore_index=True
                )
                df_sirovi = pripremi_sirove(df_sirovi)
                upisi_u_bigquery(df_sirovi, TABLE_SIROVI)
                oznaci_upisane(novi_s)
            else:
                print("  Nema novih sirovih podataka.")

            novi_m1 = novi_fajlovi(FOLDER_METRIKA1)
            novi_m2 = novi_fajlovi(FOLDER_METRIKA2)
            novi_m3 = novi_fajlovi(FOLDER_METRIKA3)

            if novi_m1:
                try:
                    df_m1 = pd.concat([pd.read_csv(f) for f in novi_m1], ignore_index=True)
                    df_zone = pripremi_timestamp_kolone(df_m1)
                    upisi_u_bigquery(df_zone, TABLE_ZONE)
                    oznaci_upisane(novi_m1)
                except Exception as e:
                    print(f"  Greška pri čitanju metrika1: {e}")
            else:
                print("  Nema novih podataka za zone.")
            
            if novi_m2:
                try:
                    df_m2 = pd.concat([pd.read_csv(f) for f in novi_m2], ignore_index=True)
                    df_vozila = pripremi_timestamp_kolone(df_m2)
                    upisi_u_bigquery(df_vozila, TABLE_VOZILA)
                    oznaci_upisane(novi_m2)
                except Exception as e:
                    print(f"  Greška pri čitanju metrika2: {e}")
            else:
                print("  Nema novih podataka za vozila.")

            if novi_m3:
                try:
                    df_m3 = pd.concat([pd.read_csv(f) for f in novi_m3], ignore_index=True)
                    df_placanje = pripremi_timestamp_kolone(df_m3)
                    upisi_u_bigquery(df_placanje, TABLE_PLACANJE)
                    oznaci_upisane(novi_m3)
                except Exception as e:
                    print(f"  Greška pri čitanju metrika3: {e}")
            else:
                print("  Nema novih podataka za plaćanje.")

            print(f"  Sledeći upis za {INTERVAL_SEC}s...")
            time.sleep(INTERVAL_SEC)

    except KeyboardInterrupt:
        print(f"\nUploader zaustavljen nakon {iteracija} iteracija.")


if __name__ == "__main__":
    main()