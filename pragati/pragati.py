import numpy as np
import matplotlib.pyplot as plt

nx, ny = 1440, 721  
fill_value = -1e20

lon = np.linspace(-180, 180, nx)
lat = np.linspace(-90, 90, ny)

sst = np.fromfile("C:/Users/navne/Desktop/pragati/salt.grd", dtype=np.float64).reshape((ny, nx))

# Mask land + fill values
sst = np.where(
    (sst <= 0) | (sst < fill_value) | (sst > 100),
    np.nan,
    sst
)

# Print first 20 valid values
print("First 20 values of SST:")
print(sst.flatten()[:12])

lon2d, lat2d = np.meshgrid(lon, lat)

plt.figure(figsize=(12, 6))
pcm = plt.pcolormesh(
    lon2d, lat2d, sst,
    cmap="jet",
    shading="auto"
)
plt.colorbar(pcm, label="Wind Speed (m/s)")
plt.title("Wind Speed (Land masked as NaN)")
plt.xlabel("Longitude")
plt.ylabel("Latitude")
plt.show()