import pandas as pd
from geopy.geocoders import Nominatim
from geopy.exc import GeocoderTimedOut, GeocoderUnavailable
import time

# Dizionario di coordinate per i paesi più comuni nelle missioni italiane
COUNTRY_COORDINATES = {
    # Balkans
    'Kosovo': (42.6026, 20.9030),
    'Serbia': (44.0165, 21.0059),
    'Bosnia and Herzegovina': (43.9159, 17.6791),
    'Croatia': (45.1000, 15.2000),
    'Montenegro': (42.7087, 19.3744),
    'North Macedonia': (41.6086, 21.7453),
    'Albania': (41.1533, 20.1683),
    
    # Middle East
    'Lebanon': (33.8547, 35.8623),
    'Iraq': (33.2232, 43.6793),
    'Syria': (34.8021, 38.9968),
    'Israel': (31.0461, 34.8516),
    'Palestine': (31.9522, 35.2332),
    'Jordan': (30.5852, 36.2384),
    'Yemen': (15.5527, 48.5164),
    
    # Africa
    'Mali': (17.5707, -3.9962),
    'Central African Republic': (6.6111, 20.9394),
    'Democratic Republic of the Congo': (-4.0383, 21.7587),
    'South Sudan': (6.8770, 31.3070),
    'Somalia': (5.1521, 46.1996),
    'Libya': (26.3351, 17.2283),
    'Sudan': (12.8628, 30.2176),
    'Chad': (15.4542, 18.7322),
    'Niger': (17.6078, 8.0817),
    'Burkina Faso': (12.2383, -1.5616),
    'Côte d\'Ivoire': (7.5400, -5.5471),
    'Senegal': (14.4974, -14.4524),
    'Guinea-Bissau': (11.8037, -15.1804),
    'Sierra Leone': (8.4606, -11.7799),
    'Liberia': (6.4281, -9.4295),
    'Ghana': (7.9465, -1.0232),
    'Togo': (8.6195, 0.8248),
    'Benin': (9.3077, 2.3158),
    'Nigeria': (9.0820, 8.6753),
    'Cameroon': (7.3697, 12.3547),
    'Gabon': (-0.8037, 11.6094),
    'Congo': (-0.2280, 15.8277),
    'Angola': (-11.2027, 17.8739),
    'Zambia': (-13.1339, 27.8493),
    'Zimbabwe': (-19.0154, 29.1549),
    'Mozambique': (-18.6657, 35.5296),
    'Madagascar': (-18.7669, 46.8691),
    'Comoros': (-11.6455, 43.3333),
    'Djibouti': (11.8251, 42.5903),
    'Eritrea': (15.1794, 39.7823),
    'Ethiopia': (9.1450, 40.4897),
    'Kenya': (-0.0236, 37.9062),
    'Tanzania': (-6.1730, 35.7389),
    'Uganda': (1.3733, 32.2903),
    'Rwanda': (-1.9403, 30.0596),
    'Burundi': (-3.3731, 29.9189),
    'Malawi': (-13.2543, 34.3015),
    'Namibia': (-22.9576, 18.4904),
    'Botswana': (-22.3285, 24.6849),
    'South Africa': (-30.5595, 22.9375),
    'Lesotho': (-29.6099, 28.2336),
    'Eswatini': (-26.5225, 31.4659),
    
    # Asia
    'Afghanistan': (33.9391, 67.7100),
    'Pakistan': (30.3753, 69.3451),
    'India': (20.5937, 78.9629),
    'Nepal': (28.3949, 84.1240),
    'Bangladesh': (23.6850, 90.3563),
    'Myanmar': (21.9162, 95.9560),
    'Thailand': (15.8700, 100.9925),
    'Cambodia': (12.5657, 104.9910),
    'Vietnam': (14.0583, 108.2772),
    'Laos': (19.8563, 102.4955),
    'Philippines': (12.8797, 121.7740),
    'Indonesia': (-0.7893, 113.9213),
    'Malaysia': (4.2105, 108.9758),
    'Singapore': (1.3521, 103.8198),
    'Brunei': (4.5353, 114.7277),
    'East Timor': (-8.8742, 125.7275),
    
    # Europe
    'Georgia': (42.3154, 43.3569),
    'Armenia': (40.0691, 45.0382),
    'Azerbaijan': (40.1431, 47.5769),
    'Ukraine': (48.3794, 31.1656),
    'Moldova': (47.4116, 28.3699),
    'Romania': (45.9432, 24.9668),
    'Bulgaria': (42.7339, 25.4858),
    'Greece': (39.0742, 21.8243),
    'Turkey': (38.9637, 35.2433),
    'Cyprus': (35.1264, 33.4299),
    
    # Americas
    'Haiti': (18.9712, -72.2852),
    'Dominican Republic': (18.7357, -70.1627),
    'Cuba': (21.5218, -77.7812),
    'Jamaica': (18.1096, -77.2975),
    'Trinidad and Tobago': (10.6590, -61.5190),
    'Guyana': (4.8604, -58.9302),
    'Suriname': (3.9193, -56.0278),
    'Brazil': (-14.2350, -51.9253),
    'Argentina': (-38.4161, -63.6167),
    'Chile': (-35.6751, -71.5430),
    'Peru': (-9.1900, -75.0152),
    'Ecuador': (-1.8312, -78.1834),
    'Colombia': (4.5709, -74.2973),
    'Venezuela': (6.4238, -66.5897),
    'Panama': (8.5380, -80.7821),
    'Costa Rica': (9.9281, -84.0907),
    'Nicaragua': (12.8654, -85.2072),
    'Honduras': (15.1999, -86.2419),
    'El Salvador': (13.7942, -88.8965),
    'Guatemala': (15.7835, -90.2308),
    'Belize': (17.1899, -88.4976),
    'Mexico': (23.6345, -102.5528),
    
    # Oceania
    'Papua New Guinea': (-6.3150, 143.9555),
    'Solomon Islands': (-9.6457, 160.1562),
    'Vanuatu': (-15.3767, 166.9592),
    'Fiji': (-17.7134, 178.0650),
    'New Caledonia': (-20.9043, 165.6180),
    
    # Mediterranean
    'Malta': (35.9375, 14.3754),
    'Tunisia': (33.8869, 9.5375),
    'Algeria': (28.0339, 1.6596),
    'Morocco': (31.7917, -7.0926),
    'Egypt': (26.8206, 30.8025),
    
    # Default per paesi non mappati
    'Italy': (41.8719, 12.5674),  # Roma
}

def get_country_coordinates(country_name):
    """
    Ottiene le coordinate per un paese specifico.
    Prima controlla il dizionario, poi usa geocoding se necessario.
    """
    # Normalizza il nome del paese
    country_name = str(country_name).strip()
    
    # Controlla il dizionario
    if country_name in COUNTRY_COORDINATES:
        return COUNTRY_COORDINATES[country_name]
    
    # Prova varianti comuni
    for key, coords in COUNTRY_COORDINATES.items():
        if country_name.lower() in key.lower() or key.lower() in country_name.lower():
            return coords
    
    # Se non trovato, usa geocoding (con fallback)
    try:
        geolocator = Nominatim(user_agent="mida_dashboard")
        location = geolocator.geocode(country_name, timeout=10)
        if location:
            return (location.latitude, location.longitude)
    except (GeocoderTimedOut, GeocoderUnavailable):
        pass
    
    # Fallback: coordinate di Roma
    return (41.8719, 12.5674)

def add_coordinates_to_dataframe(df):
    """
    Aggiunge colonne lat/lon al DataFrame basandosi sul paese.
    """
    if 'paese' not in df.columns:
        # Se non c'è colonna paese, usa coordinate di default
        df['lat'] = 41.8719
        df['lon'] = 12.5674
        return df
    
    # Ottieni coordinate per ogni paese
    coordinates = df['paese'].apply(get_country_coordinates)
    df['lat'] = [coord[0] for coord in coordinates]
    df['lon'] = [coord[1] for coord in coordinates]
    
    return df 