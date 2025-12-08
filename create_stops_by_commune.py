"""
Script pour créer une base de données du nombre d'arrêts de transport par commune.
Utilise une jointure spatiale avec les contours officiels des communes françaises.
"""

import pandas as pd
import geopandas as gpd
from shapely.geometry import Point
import requests
import os

# ============================================================
# ÉTAPE 1 : Télécharger les contours des communes (si pas déjà fait)
# ============================================================

COMMUNES_FILE = "communes-france.geojson"

def download_communes():
    """Télécharge les contours des communes depuis data.gouv.fr"""
    if os.path.exists(COMMUNES_FILE):
        print(f"✓ Fichier {COMMUNES_FILE} déjà présent")
        return
    
    print("⏳ Téléchargement des contours des communes (~50 Mo)...")
    print("   Source : data.gouv.fr (OpenStreetMap)")
    
    # Fichier GeoJSON des communes depuis data.gouv.fr
    # Source : https://www.data.gouv.fr/fr/datasets/decoupage-administratif-communal-francais-issu-d-openstreetmap/
    url = "https://raw.githubusercontent.com/gregoiredavid/france-geojson/master/communes.geojson"
    
    response = requests.get(url, stream=True)
    response.raise_for_status()
    
    # Télécharger avec progress
    total_size = int(response.headers.get('content-length', 0))
    downloaded = 0
    
    with open(COMMUNES_FILE, 'wb') as f:
        for chunk in response.iter_content(chunk_size=8192):
            f.write(chunk)
            downloaded += len(chunk)
            if total_size:
                pct = (downloaded / total_size) * 100
                print(f"\r   Progression : {pct:.1f}%", end="", flush=True)
    
    print(f"\n✓ Contours téléchargés et sauvegardés dans {COMMUNES_FILE}")

# ============================================================
# ÉTAPE 2 : Charger les arrêts de transport
# ============================================================

def load_stops(csv_file):
    """Charge le fichier CSV des arrêts"""
    print(f"⏳ Chargement des arrêts depuis {csv_file}...")
    
    stops = pd.read_csv(csv_file)
    print(f"✓ {len(stops)} arrêts chargés")
    
    # Supprimer les lignes sans coordonnées valides
    stops = stops.dropna(subset=['stop_lat', 'stop_lon'])
    print(f"✓ {len(stops)} arrêts avec coordonnées valides")
    
    return stops

# ============================================================
# ÉTAPE 3 : Convertir en GeoDataFrame et faire la jointure spatiale
# ============================================================

def create_stops_geodataframe(stops):
    """Convertit les arrêts en GeoDataFrame avec des points"""
    print("⏳ Création des points géographiques...")
    
    geometry = [Point(lon, lat) for lon, lat in zip(stops['stop_lon'], stops['stop_lat'])]
    stops_gdf = gpd.GeoDataFrame(stops, geometry=geometry, crs="EPSG:4326")
    
    print(f"✓ GeoDataFrame créé avec {len(stops_gdf)} points")
    return stops_gdf

def spatial_join(stops_gdf, communes_gdf):
    """Fait la jointure spatiale entre arrêts et communes"""
    print("⏳ Jointure spatiale en cours (peut prendre quelques minutes)...")
    
    # S'assurer que les deux ont le même CRS
    if communes_gdf.crs != stops_gdf.crs:
        communes_gdf = communes_gdf.to_crs(stops_gdf.crs)
    
    # Jointure spatiale : chaque point → la commune qui le contient
    joined = gpd.sjoin(stops_gdf, communes_gdf, how='left', predicate='within')
    
    # Identifier les arrêts non matchés
    unmatched_mask = joined['code'].isna()
    n_unmatched = unmatched_mask.sum()
    print(f"   Arrêts matchés directement : {len(joined) - n_unmatched}")
    print(f"   Arrêts non matchés : {n_unmatched}")
    
    if n_unmatched > 0:
        print("⏳ Recherche des communes les plus proches pour les arrêts non matchés...")
        
        # Filtrer les arrêts non matchés avec coordonnées valides (pas 0,0)
        unmatched_stops = stops_gdf[unmatched_mask].copy()
        
        # Exclure les coordonnées (0,0) et hors France métropolitaine
        valid_coords_mask = (
            (unmatched_stops.geometry.x != 0) & 
            (unmatched_stops.geometry.y != 0) &
            (unmatched_stops.geometry.y >= 41) & 
            (unmatched_stops.geometry.y <= 52) &
            (unmatched_stops.geometry.x >= -6) & 
            (unmatched_stops.geometry.x <= 10)
        )
        
        unmatched_valid = unmatched_stops[valid_coords_mask]
        n_invalid = (~valid_coords_mask).sum()
        print(f"   Arrêts avec coordonnées invalides/DOM-TOM ignorés : {n_invalid}")
        print(f"   Arrêts à matcher par proximité : {len(unmatched_valid)}")
        
        if len(unmatched_valid) > 0:
            # Jointure par plus proche voisin
            joined_nearest = gpd.sjoin_nearest(
                unmatched_valid, 
                communes_gdf, 
                how='left',
                distance_col='distance_to_commune'
            )
            
            # Stats sur les distances
            if 'distance_to_commune' in joined_nearest.columns:
                max_dist = joined_nearest['distance_to_commune'].max()
                mean_dist = joined_nearest['distance_to_commune'].mean()
                print(f"   Distance moyenne à la commune : {mean_dist:.4f}° (~{mean_dist * 111:.1f} km)")
                print(f"   Distance max : {max_dist:.4f}° (~{max_dist * 111:.1f} km)")
            
            # Mettre à jour les arrêts non matchés avec les résultats du nearest
            for col in communes_gdf.columns:
                if col != 'geometry' and col in joined_nearest.columns:
                    joined.loc[joined_nearest.index, col] = joined_nearest[col]
            
            print(f"✓ {len(unmatched_valid)} arrêts assignés à la commune la plus proche")
    
    print(f"✓ Jointure terminée")
    return joined

# ============================================================
# ÉTAPE 4 : Compter les arrêts par commune
# ============================================================

def count_stops_by_commune(joined_gdf):
    """Compte le nombre d'arrêts par commune"""
    print("⏳ Comptage des arrêts par commune...")
    
    # Identifier les colonnes disponibles (peuvent varier selon la source)
    # Chercher le code INSEE et le nom de la commune
    code_col = None
    nom_col = None
    
    for col in joined_gdf.columns:
        col_lower = col.lower()
        if col_lower in ['code', 'code_insee', 'codgeo', 'insee']:
            code_col = col
        if col_lower in ['nom', 'name', 'libelle', 'nom_commune']:
            nom_col = col
    
    if code_col is None:
        print("⚠️  Colonnes disponibles:", list(joined_gdf.columns))
        raise ValueError("Impossible de trouver la colonne du code INSEE")
    
    print(f"   Utilisation des colonnes : code={code_col}, nom={nom_col}")
    
    # Filtrer les lignes sans code (arrêts hors France)
    joined_gdf = joined_gdf.dropna(subset=[code_col])
    
    # Compter par code INSEE
    if nom_col:
        result = joined_gdf.groupby([code_col, nom_col]).size().reset_index(name='nb_arrets')
        result = result.rename(columns={code_col: 'code_commune_INSEE', nom_col: 'nom_commune'})
    else:
        result = joined_gdf.groupby([code_col]).size().reset_index(name='nb_arrets')
        result = result.rename(columns={code_col: 'code_commune_INSEE'})
        result['nom_commune'] = ''
    
    # Extraire le code département (2 premiers chiffres, ou 3 pour DOM-TOM)
    def get_dept_code(code_insee):
        code_str = str(code_insee)
        if code_str.startswith('97') or code_str.startswith('98'):
            return code_str[:3]  # DOM-TOM
        return code_str[:2]
    
    result['code_departement'] = result['code_commune_INSEE'].apply(get_dept_code)
    
    # Créer la colonne codeDepartement-codeCommune
    result['code_dept_commune'] = result['code_departement'] + '-' + result['code_commune_INSEE'].astype(str)
    
    # Réordonner les colonnes
    result = result[['code_dept_commune', 'code_departement', 'code_commune_INSEE', 'nom_commune', 'nb_arrets']]
    
    # Trier par nombre d'arrêts décroissant
    result = result.sort_values('nb_arrets', ascending=False)
    
    print(f"✓ {len(result)} communes avec au moins 1 arrêt")
    return result

# ============================================================
# MAIN
# ============================================================

def main():
    # 1. Télécharger les contours des communes
    try:
        download_communes()
    except Exception as e:
        print(f"\n❌ Erreur de téléchargement : {e}")
        print("\n📥 Télécharge manuellement le fichier depuis :")
        print("   https://github.com/gregoiredavid/france-geojson/raw/master/communes.geojson")
        print(f"   Et sauvegarde-le sous le nom : {COMMUNES_FILE}")
        return
    
    # 2. Charger les communes
    print("⏳ Chargement des contours des communes...")
    communes = gpd.read_file(COMMUNES_FILE)
    print(f"✓ {len(communes)} communes chargées")
    print(f"   Colonnes disponibles : {list(communes.columns)}")
    
    # 3. Charger les arrêts
    stops = load_stops("accessibilite-brute.csv")
    
    # 4. Convertir en GeoDataFrame
    stops_gdf = create_stops_geodataframe(stops)
    
    # 5. Jointure spatiale
    joined = spatial_join(stops_gdf, communes)
    
    # 6. Compter par commune
    result = count_stops_by_commune(joined)
    
    # 7. Sauvegarder le résultat
    output_file = "arrets_par_commune.csv"
    result.to_csv(output_file, index=False, encoding='utf-8')
    print(f"\n✅ Résultat sauvegardé dans {output_file}")
    
    # Afficher un aperçu
    print("\n📊 Top 20 des communes avec le plus d'arrêts :")
    print(result.head(20).to_string(index=False))
    
    # Stats
    total_arrets_result = result['nb_arrets'].sum()
    print(f"\n📈 Statistiques :")
    print(f"   - Total arrêts dans le résultat : {total_arrets_result}")
    print(f"   - Arrêts dans le fichier source : {len(stops)}")
    print(f"   - Taux de matching : {total_arrets_result/len(stops)*100:.1f}%")
    print(f"   - Communes avec arrêts : {len(result)}")
    print(f"   - Communes sans arrêts : {len(communes) - len(result)}")
    print(f"   - Moyenne par commune : {result['nb_arrets'].mean():.1f}")
    print(f"   - Max : {result['nb_arrets'].max()} ({result.iloc[0]['nom_commune']})")

if __name__ == "__main__":
    main()

