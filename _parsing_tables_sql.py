#importer les librairies
import pandas as pd
import re
import glob
import warnings
import os 

# Chemin du répertoire contenant les fichiers
chemin_repertoire = "C:/Talend/ATP/PROJETS/"

# Modèle pour les fichiers que vous souhaitez lire
modele_fichier = "*_query.xlsx"

# Utiliser glob pour obtenir la liste des fichiers correspondants au modèle
chemin_fichiers = glob.glob(chemin_repertoire + modele_fichier)

# Initialiser un DataFrame vide pour stocker les données de tous les fichiers
df = pd.DataFrame()

with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")

# Boucle pour lire chaque fichier, ajouter une colonne avec le nom du fichier, et concaténer les données
    for fichier in chemin_fichiers:
        df_temp = pd.read_excel(fichier)
        
        # Extraire le nom du fichier (sans le chemin)
        nom_fichier = fichier.split("\\")[-1]
        nom_fichier= nom_fichier.split(".")[0]
        
        # Ajouter une colonne "Nom_Fichier" au DataFrame temporaire
        df_temp = df_temp.assign(projet=nom_fichier)
        
        # Concaténer les données dans le DataFrame global
        df = pd.concat([df, df_temp], ignore_index=True)

# Supprimer tous les fichiers Excel après le traitement
for fichier in chemin_fichiers:
    os.remove(fichier)
    print(f'Le fichier {fichier} a ete supprime.')

print("Le fichier Excel a ete traite et supprime.")


#créer les fonctions regex
def extraire_noms_tables(requete_sql):
    # Utilisation d'une expression régulière pour trouver les occurrences de motifs de nom de table
    motif = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
    matches = re.findall(motif, requete_sql, flags=re.IGNORECASE)

    # Regrouper les résultats de toutes les expressions régulières
    noms_tables = [table for match in matches for table in match if table]

    return noms_tables

def extract_old_sql(requete_sql):
    # Recherche de l'indice du mot "FROM"
    index_from = requete_sql.upper().find("FROM")
    
    if index_from != -1:
        # Extrait la sous-chaîne après le mot "FROM"
        sous_chaine = requete_sql[index_from + 4:]
        
        # Supprime les espaces en début et en fin de chaîne
        sous_chaine = sous_chaine.strip()
        
        # Recherche de l'indice du mot "WHERE"
        index_where = sous_chaine.upper().find("WHERE")
        
        if index_where != -1:
            # Extrait la sous-chaîne avant le mot "WHERE"
            sous_chaine_tables = sous_chaine[:index_where].strip()
            
            # Vérifie s'il y a une virgule dans la sous-chaîne
            if ',' in sous_chaine_tables:
                # Sépare les noms de table en fonction des virgules
                noms_tables = [table.strip() for table in sous_chaine_tables.split(',')]
                return noms_tables
            
def mettre_none_si_commence_par_parenthese(valeur):
    if isinstance(valeur, str) and valeur.startswith("("):
        return None
    else:
        return valeur

def modifier_truncate(phrase):
    mots = phrase.split()  # Divisez la phrase en mots
    mots_modifies = [mot.upper() if mot.lower() == 'truncate' else mot for mot in mots]
    return ' '.join(mots_modifies)  # Rejoignez les mots pour former la phrase modifiée
   
def replace_table (row):    
    if 'TRUNCATE' in row['requete_sql']:        
        return row['requete_sql']  
    else: return row['table']


# Lancement du traitement
print("Parsing en cours d'execution ! :)  ------------")

# Modification du type en string pour la colonne requête sql
df['requete_sql']=df['requete_sql'].astype(str)

# Supprimer les requêtes inutiles
df['requete_sql']= df['requete_sql'].replace("select id  name from employee", "requete non modifiee")

# Appliquez la fonction modifier truncate à la colonne requete_sql
df['requete_sql'] = df['requete_sql'].apply(modifier_truncate)

# remplace les "\" dans les lignes (plusieurs fois)
nb_boucle = 20
for i in range(nb_boucle):
    df['requete_sql'] = df['requete_sql'].str.replace('\\', ' ').str.replace('\'', ' ').str.replace('\n',' ').str.replace('\t', ' ')

# Appliquer la fonction personnalisée à chaque ligne de la colonne 'requete_sql'
df['table'] = df['requete_sql'].apply(extraire_noms_tables)

# Appliquer la fonction à chaque ligne du DataFrame
df['table'] = df.apply(replace_table, axis=1)

# S'assurer que la colonne 'table' est de type chaîne
df['table'] = df['table'].astype(str)

# Supprimer les crochets de la colonne 'table'
df['table']=df['table'].str.replace('[', '').str.replace(']', '')

# Supprimer tous les guillemets simples de la colonne 'table'
df['table'] = df['table'].str.replace('\'', '')

# Appliquer la fonction personnalisée à chaque ligne de la colonne 'requete_sql'
df['table_2'] = df['requete_sql'].apply(extract_old_sql)

# S'assurer que la colonne 'table' est de type chaîne
df['table_2'] = df['table_2'].astype(str)

# Supprimer les crochets de la colonne 'table'
df['table_2']=df['table_2'].str.replace('[', '').str.replace(']', '')

# Supprimer tous les guillemets simples de la colonne 'table'
df['table_2'] = df['table_2'].str.replace('\'', '')

# Appliquer la fonction à la colonne 'table_2'
df['table_2'] = df['table_2'].apply(mettre_none_si_commence_par_parenthese)

# Remplacer les valeurs nulles (None) dans la colonne "table_2" par celles de la colonne "table"
df['table_2'] = df['table_2'].apply(lambda x: x if x != 'None' else None)
df['table_2'].fillna(df['table'], inplace=True)

#Supprimer le mot TRUNCATE table de la colonne table2
df['table_2'] = df['table_2'].str.replace('TRUNCATE table', '')

#Supprimer le mot TRUNCATE de la colonne table2
df['table_2'] = df['table_2'].str.replace('TRUNCATE TABLE', '')

#Supprimer le mot TRUNCATE de la colonne table2
df['table_2'] = df['table_2'].str.replace('TRUNCATE', '')

# Supprimer la colonne "table"
df.drop('table', axis=1, inplace=True)

#Remplacer les cellules vides de id_job par NC
#df['id_job']=df['id_job'].fillna("NC")

#Supprimer les retours à la ligne
for column in df.columns:
    df[column] = df[column].astype(str).str.replace('\n', ' ')
    df[column] = df[column].astype(str).str.replace('\r', ' ')
    #df[column] = df[column].astype(str).str.replace(',', ';')

    
# Remplacer les virgules par des points virgules
df['table_2'] = df['table_2'].astype(str).str.replace(',', ';')

# Diviser les valeurs de la colonne contenant des points-virgules
df['table_2'] = df['table_2'].str.split(';')

# Utiliser la fonction explode pour déplier les listes résultantes
df = df.explode('table_2')


df.to_csv("C:/Talend/ATP/PROJETS/job_tables_sql.csv", index=False)

print("Le fichier csv est pret pour etre integre au job J025 Talend -->")