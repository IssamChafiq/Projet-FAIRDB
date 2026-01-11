
import time
import requests as req

"""
Comment créer une clé d’API pour requêter l’API SIRENE:
https://portail-api.insee.fr/catalog/api/2ba0e549-5587-3ef1-9082-99cd865de66f/doc?page=85c5657d-b1a1-4466-8565-7db1a194667b#usage-de-la-clé-dapi-pour-requêter-lapi-sirene

Swagger documentation:
https://portail-api.insee.fr/catalog/api/2ba0e549-5587-3ef1-9082-99cd865de66f/doc?page=6548510e-c3e1-3099-be96-6edf02870699
--> Etablissements --> GET /siret

Pour tester tes requêtes, tu peux utiliser le 'try it out' du swagger, ça te permet aussi d'obtenir directement l'url de ta requête.

Exemple de mon url:
https://api.insee.fr/api-sirene/3.11/siret?date=2023-01-01&champs=activitePrincipaleEtablissement%2C%20codeCommuneEtablissement%2C%20libelleCommuneEtablissement&nombre=1000
"""


# Transform JSON data to CSV format and append (or create) to file
def transformation(json_data, header=False):
    # Je te conseille de changer de fichier tous les 5 millions de lignes pour pas que ça lag.
    f = open('data3.csv', 'a')

    # If first line of the file, write names of columns
    if(header): f.write('activitePrincipaleEtablissement,codeCommuneEtablissement,libelleCommuneEtablissement\n')

    # Iterate through each item in JSON data
    for item in json_data:
        # If you look at the swagger, you'll see that activitePrincipaleEtablissement is nested in periodesEtablissement so you first
        # have to get periodesEtablissement (which is a list with most of the time only one element, don't get confused) and then get
        # activitePrincipaleEtablissement from the first element of that list
        activite = item.get('periodesEtablissement')[0].get('activitePrincipaleEtablissement')

        # Same for adresseEtablissement (but not a list) which contains codeCommuneEtablissement and libelleCommuneEtablissement
        adresse = item.get('adresseEtablissement')
        code_commune = adresse.get('codeCommuneEtablissement')
        libelle_commune = adresse.get('libelleCommuneEtablissement')

        # Write the extracted data to the CSV file (just a string formatted as CSV, careful to respect the order of columns you put as a header)
        f.write(f'{activite},{code_commune},{libelle_commune}\n')
    f.close()

# Get data from API using cursor for pagination
# In case you don't know what a cursor is, it's kind of a pointer that tells the API where to continue fetching data from
# (because your requests are limited to only 1000 records at a time)
def get_with_cursor(url, headers):
    cursor = ''

    # Initial request with a starting cursor
    # Si t'as un crash à un moment, tu peux reprendre à partir d'u dernier curseur en le mettant à la place de la variable cursor
    response = req.get(
        url + '&curseur=' + cursor,
        headers=headers
    ).json()

    # Si t'as un crash et que t'as perdu le dernier curseur, tu peux prendre le dernier curseur que tu connais, et changer le i ici pour
    # pas réécrire les lignes dans le fichier csv.
    # Le i représente le nombre de requêtes que t'as déjà faites (va compter le nombre de lignes que t'as dans ton fichier csv) i = 1 <=> 1000 lignes
    i = 17090

    # Get the next cursor from the response header
    cursor = response['header'].get('curseurSuivant')

    # Si tu reprendre après un crash (et donc que t'es encore dans le même fichier csv), retire le True pour ne pas réécrire les noms des
    # colonnes au milieu du fichier
    transformation(response['etablissements'], True)

    # While there are still records to fetch
    while response.get('nombre') != 0:

        # Handle rate limiting: pause after every 30 requests
        if(i % 30 == 0):
            print(f'i = {i}. Waiting for rate limit reset...')
            time.sleep(60)
            print('Resuming...')
        
        # Make the next request using the updated cursor
        response = req.get(
            url + '&curseur=' + cursor,
            headers=headers
        ).json()
        i += 1

        # Update the cursor for the next iteration
        cursor = response['header'].get('curseurSuivant')

        # print the current cursor to keep track of progress in case of crashes (we're not writing them to a file because it would be 
        # as big as the file of the data)
        print(cursor)

        # Transform and save the fetched data
        transformation(response['etablissements'])

# Start the data fetching process
get_with_cursor(
    'URL ICI',
    headers={'X-INSEE-Api-Key-Integration': ''' mettre ta clef ici '''}
)