"""Sonda de la Data API: batch servido y solapamiento de filas entre peticiones."""
import json, sys, time, urllib.request

URL = sys.argv[1] if len(sys.argv) > 1 else "http://localhost:8081"
GROUP = 2

def pedir():
    with urllib.request.urlopen(f"{URL}/data?group_number={GROUP}") as r:
        d = json.load(r)
    return d["batch_number"], {tuple(f) for f in d["data"]}

b1, s1 = pedir()
b2, s2 = pedir()
print(f"Petición 1: batch {b1}, {len(s1)} filas")
print(f"Petición 2 (inmediata): batch {b2}, {len(s2)} filas, repetidas con la 1: {len(s1 & s2)}")
print("Esperando 35 s...")
time.sleep(35)
b3, s3 = pedir()
print(f"Petición 3 (+35 s): batch {b3}, {len(s3)} filas, repetidas con la 1: {len(s1 & s3)}")
