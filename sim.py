import json
import time
import random
import os
from datetime import datetime



OUTPUT_DIR = "stream_data"  
INTERVAL_SEC = 5            

# Parking mesta po zonama i lokacijama
PARKING_MESTA = [
    # Zona I
    {"sensor_id": "P01", "zona": "Zona I",   "lokacija": "Trg kralja Milana"},
    {"sensor_id": "P02", "zona": "Zona I",   "lokacija": "Trg kralja Milana"},
    {"sensor_id": "P03", "zona": "Zona I",   "lokacija": "Obrenovićeva ulica"},
    {"sensor_id": "P04", "zona": "Zona I",   "lokacija": "Obrenovićeva ulica"},
    {"sensor_id": "P05", "zona": "Zona I",   "lokacija": "Vožd Karađorđeva"},
    {"sensor_id": "P06", "zona": "Zona I",   "lokacija": "Vožd Karađorđeva"},

    # Zona II 
    {"sensor_id": "P07", "zona": "Zona II",  "lokacija": "Bulevar Nemanjića"},
    {"sensor_id": "P08", "zona": "Zona II",  "lokacija": "Bulevar Nemanjića"},
    {"sensor_id": "P09", "zona": "Zona II",  "lokacija": "Vojvode Putnika"},
    {"sensor_id": "P10", "zona": "Zona II",  "lokacija": "Vojvode Putnika"},
    {"sensor_id": "P11", "zona": "Zona II",  "lokacija": "Sinđelićev trg"},
    {"sensor_id": "P12", "zona": "Zona II",  "lokacija": "Sinđelićev trg"},

    # Zona III
    {"sensor_id": "P13", "zona": "Zona III", "lokacija": "Bulevar dr Zorana Đinđića"},
    {"sensor_id": "P14", "zona": "Zona III", "lokacija": "Bulevar dr Zorana Đinđića"},
    {"sensor_id": "P15", "zona": "Zona III", "lokacija": "Duška Pančića"},
    {"sensor_id": "P16", "zona": "Zona III", "lokacija": "Duška Pančića"},
    {"sensor_id": "P17", "zona": "Zona III", "lokacija": "Cara Dušana"},
    {"sensor_id": "P18", "zona": "Zona III", "lokacija": "Cara Dušana"},
    {"sensor_id": "P19", "zona": "Zona III", "lokacija": "Kosovska ulica"},
    {"sensor_id": "P20", "zona": "Zona III", "lokacija": "Kosovska ulica"},
]

TIPOVI_VOZILA = ["auto", "motor", "kombi"]
NACINI_PLACANJA = ["kartica", "gotovina", "app"]


ZAUZETOST_VEROVATNOCA = {
    "Zona I":   0.85,
    "Zona II":  0.65,
    "Zona III": 0.45,
}



def generiši_očitavanje(mesto: dict) -> dict:
    zona = mesto["zona"]
    zauzeto = random.random() < ZAUZETOST_VEROVATNOCA[zona]

    return {
        "sensor_id":   mesto["sensor_id"],
        "zona":        zona,
        "lokacija":    mesto["lokacija"],
        "status":      "zauzeto" if zauzeto else "slobodno",
        "vozilo_tip":  random.choice(TIPOVI_VOZILA) if zauzeto else None,
        "placanje":    random.choice(NACINI_PLACANJA) if zauzeto else None,
        "timestamp":   datetime.now().strftime("%Y-%m-%dT%H:%M:%S"),
    }


def sacuvaj_batch(podaci: list, batch_id: int):
    os.makedirs(OUTPUT_DIR, exist_ok=True)
    timestamp_str = datetime.now().strftime("%Y%m%d_%H%M%S_%f")
    naziv_fajla = os.path.join(OUTPUT_DIR, f"batch_{timestamp_str}.json")
    with open(naziv_fajla, "w", encoding="utf-8") as f:
        for red in podaci:
            f.write(json.dumps(red, ensure_ascii=False) + "\n")
    print(f"[{datetime.now().strftime('%H:%M:%S')}] Batch sačuvan {batch_id}"
          f"— {len(podaci)} očitavanja → {naziv_fajla}")




def main():
    print("=" * 55)
    print("  Parking Simulator — Niš")
    print(f"  Mesta: {len(PARKING_MESTA)} | Interval: {INTERVAL_SEC}s")
    print(f"  Izlazni folder: {OUTPUT_DIR}/")
    print("=" * 55)

    batch_id = 0
    try:
        while True:
            batch = [generiši_očitavanje(mesto) for mesto in PARKING_MESTA]
            sacuvaj_batch(batch, batch_id)
            batch_id += 1
            time.sleep(INTERVAL_SEC)
    except KeyboardInterrupt:
        print(f"\nSimulator zaustavljen. Generisano {batch_id} batch-eva.")


if __name__ == "__main__":
    main()