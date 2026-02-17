import xarray as xr
import matplotlib.pyplot as plt
from matplotlib.animation import FuncAnimation, PillowWriter

# Ouvrir le fichier
ds = xr.open_dataset("04_SAFRAN-data_output/TINF_H_QUOT_SIM2_latest-19580801-20260215.nc")

# Prendre les données (tu peux ajuster le slice)
data = ds['TINF_H'].isel(time=slice(0, 365))  # première année

# Calculer les limites globales pour la colorbar
vmin = float(data.min().values)
vmax = float(data.max().values)

# Créer la figure
fig, ax = plt.subplots(figsize=(10, 8))


def update(frame):
    ax.clear()
    date_str = str(data.time.isel(time=frame).dt.strftime('%Y-%m-%d').values)
    im = ax.imshow(data.isel(time=frame).values, cmap='RdYlBu_r', vmin=vmin, vmax=vmax, aspect='equal')
    ax.set_title(f"Température minimale - {date_str}")
    ax.set_aspect('equal')
    return [im]

# Créer l'animation
anim = FuncAnimation(fig, update, frames=len(data.time), interval=50, blit=True)

# Sauvegarder en GIF
print("Creating GIF... (this may take a few minutes)")
anim.save('temperature_animation.gif', writer=PillowWriter(fps=20))
print("Done! Saved as temperature_animation.gif")

plt.close()
ds.close()
