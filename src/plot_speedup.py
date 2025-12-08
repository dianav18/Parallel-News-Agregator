import matplotlib.pyplot as plt
import csv

threads = []
avg_times = []

with open("benchmark_results.csv") as f:
    reader = csv.DictReader(f)
    for row in reader:
        threads.append(int(row["Threads"]))
        avg_times.append(float(row["Average"]))

T1 = avg_times[0]  # timpul pentru 1 thread
speedup = [T1 / t for t in avg_times]

plt.figure(figsize=(10, 6))

# Grafic Speedup
plt.plot(threads, speedup, marker="o")
plt.xlabel("Număr thread-uri")
plt.ylabel("Speedup")
plt.title("Grafic Speedup Tema 1 APD")
plt.grid(True)
plt.xticks(threads)

plt.savefig("speedup.png", dpi=200)
print("Grafic generat → speedup.png")

# Grafic timpi
plt.figure(figsize=(10, 6))
plt.plot(threads, avg_times, marker="o")
plt.xlabel("Număr thread-uri")
plt.ylabel("Timp execuție (s)")
plt.title("Timpi de execuție Tema 1 APD")
plt.grid(True)
plt.xticks(threads)

plt.savefig("timings.png", dpi=200)
print("Grafic generat → timings.png")
