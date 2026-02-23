import folium
import pandas as pd


class Mapping():
    def __init__(self):
        pass

    def path_map(self, data_points: pd.DataFrame) -> None:

        # Przygotowanie punktów dla Folium
        path_points = data_points[['Lat', 'Lon']].values.tolist()

        # Obliczenie środka mapy
        start_lat = data_points['Lat'].mean()
        start_lon = data_points['Lon'].mean()

        m = folium.Map(location=(start_lat,start_lon),zoom_start=13)
        trail_coordinates = path_points

        folium.PolyLine(trail_coordinates, tooltip="Coast").add_to(m)

        # Dodanie znacznika na początku trasy
        folium.Marker(
            location=path_points[0], 
            popup="Start", 
            icon=folium.Icon(color='green')
        ).add_to(m)

        # Dodanie znacznika na końcu trasy
        folium.Marker(
            location=path_points[-1], 
            popup="Koniec", 
            icon=folium.Icon(color='red')
        ).add_to(m)

        m.save("map.html")