#importer les librairies
import pandas as pd
import re
import sqlparse

import warnings

with warnings.catch_warnings(record=True):
    warnings.simplefilter("always")
    df = pd.read_excel("C:/Talend/TOS_DI-8.0.1/studio/workspace/ATP_requete.xlsx", engine="openpyxl")

#créer le dataframe à partir d'un fichier csv
#df=pd.read_excel("C:/Talend/TOS_DI-8.0.1/studio/workspace/ATP_requete.xlsx")

#créer les fonctions regex
def extraire_noms_tables(requete_sql):
    # Utilisation d'une expression régulière pour trouver les occurrences de motifs de nom de table
    motif = r'\bFROM\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bJOIN\s+([a-zA-Z_][a-zA-Z0-9_]*)\b|\bINTO\s+([a-zA-Z_][a-zA-Z0-9_]*)\b'
    matches = re.findall(motif, requete_sql, flags=re.IGNORECASE)

    # Regrouper les résultats de toutes les expressions régulières
    noms_tables = [table for match in matches for table in match if table]

    return noms_tables

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
    
# Lancement du traitement

# Appliquer la fonction personnalisée à chaque ligne de la colonne 'requete_sql'
df['table'] = df['requete_sql'].apply(extraire_noms_tables)

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

# Supprimer la colonne "table"
df.drop('table', axis=1, inplace=True)

df.to_csv("C:/Talend/TOS_DI-8.0.1/studio/workspace/job_tables_sql.csv")
