import pandas as pd
import mysql.connector
from mysql.connector import Error
import re
import json

regions = {
    "AD": "Adamaoua",
    "CE": "Centre",
    "OU": "Ouest",
    "NO": "Nord",
    "NW": "Nord-Ouest",
    "SW": "Sud-Ouest",
    "SU": "Sud",
    "EN": "Extrême-Nord",
    "ES": "Est",
    "LT": "Littoral"
}

# Connexion à la base de données MySQL
def connect_db():
    try:
        connection = mysql.connector.connect(
            host='localhost',
            database='minresi_db',
            user='root',
            password=''
        )
        if connection.is_connected():
            return connection
    except Error as e:
        print(f"Erreur lors de la connexion à MySQL : {e}")
        return None

# Charger le fichier Excel
def load_excel_data(file_path):
    return pd.read_excel(file_path)

# Vérifier et nettoyer les emails
def clean_email(email, index):
    try:
        if isinstance(email, str):
            email_parts = email.split()
            for part in email_parts:
                if '@' in part:
                    return part
        return f'test{index}@minresi.cm'
    except Exception as e:
        print(f"Erreur lors du nettoyage de l'email: {e}")
        return f'test{index}@minresi.cm'

# Nettoyer le numéro de téléphone en retirant tout sauf les chiffres et les "/"
def clean_phone(phone):
    try:
        if isinstance(phone, str):
            # Utiliser une expression régulière pour garder uniquement les chiffres et les '/'
            return re.sub(r'[^0-9/]', '', phone)
        return phone
    except Exception as e:
        print(f"Erreur lors du nettoyage du téléphone: {e}")
        return None

# Insérer ou récupérer une classe d'activité
def get_activity_class_id(name, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM activity_classes WHERE name_fr = %s"
        cursor.execute(query, (name,))
        result = cursor.fetchone()
        if result:
            activity_class_id = result[0]
        else:
            insert_query = "INSERT INTO activity_classes (name_fr) VALUES (%s)"
            cursor.execute(insert_query, (name,))
            connection.commit()
            activity_class_id = cursor.lastrowid
        cursor.close()
        return activity_class_id
    except Exception as e:
        print(f"Erreur lors de l'insertion de la classe d'activité: {e}")
        return None

# Insérer ou récupérer un domaine d'application
def get_application_domain_id(name, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM applications_domains WHERE name_fr = %s"
        cursor.execute(query, (name,))
        result = cursor.fetchone()
        if result:
            application_domain_id = result[0]
        else:
            insert_query = "INSERT INTO applications_domains (name_fr) VALUES (%s)"
            cursor.execute(insert_query, (name,))
            connection.commit()
            application_domain_id = cursor.lastrowid
        cursor.close()
        return application_domain_id
    except Exception as e:
        print(f"Erreur lors de l'insertion du domaine d'application: {e}")
        return None

# Insérer ou récupérer un domaine de spécialité
def get_specialty_domain_id(name, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM specialties_domains WHERE name_fr = %s"
        cursor.execute(query, (name,))
        result = cursor.fetchone()
        if result:
            specialty_domain_id = result[0]
        else:
            insert_query = "INSERT INTO specialties_domains (name_fr) VALUES (%s)"
            cursor.execute(insert_query, (name,))
            connection.commit()
            specialty_domain_id = cursor.lastrowid
        cursor.close()
        return specialty_domain_id
    except Exception as e:
        print(f"Erreur lors de l'insertion du domaine de spécialité: {e}")
        return None

# Insérer ou récupérer une structure d'attachement
def get_attachment_structure_id(name, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM attachment_structures WHERE name_fr = %s"
        cursor.execute(query, (name,))
        result = cursor.fetchone()
        if result:
            attachment_structure_id = result[0]
        else:
            insert_query = "INSERT INTO attachment_structures (name_fr) VALUES (%s)"
            cursor.execute(insert_query, (name,))
            connection.commit()
            attachment_structure_id = cursor.lastrowid
        cursor.close()
        return attachment_structure_id
    except Exception as e:
        print(f"Erreur lors de l'insertion de la structure d'attachement: {e}")
        return None

# Gérer le sexe, remplacer les valeurs invalides par 'Autre'
def clean_sexe(sexe):
    try:
        if isinstance(sexe, str):
            if sexe == 'E':
                return 'O'
            return sexe
    except Exception as e:
        print(f"Erreur lors du nettoyage du sexe: {e}")
        return ''

# Insérer ou récupérer un statut
def get_status_id(status_name, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM status WHERE name_fr = %s"
        cursor.execute(query, (status_name,))
        result = cursor.fetchone()
        if result:
            status_id = result[0]
        else:
            insert_query = "INSERT INTO status (name_fr) VALUES (%s)"
            cursor.execute(insert_query, (status_name,))
            connection.commit()
            status_id = cursor.lastrowid
        cursor.close()
        return status_id
    except Exception as e:
        print(f"Erreur lors de l'insertion du statut: {e}")
        return None

# Récupérer l'ID de la région par son code (name_en)
def get_region_id(region_code, connection):
    try:
        cursor = connection.cursor()
        query = "SELECT id FROM regions WHERE name_fr = %s"
        cursor.execute(query, (regions[region_code],))
        result = cursor.fetchone()
        cursor.close()
        return result[0] if result else None
    except Exception as e:
        print(f"Erreur lors de la récupération de la région: {e}")
        return None

# Insérer ou récupérer un type (procédé/produit)
def get_type_id(type_name, connection):
    cursor = connection.cursor()
    query = "SELECT id FROM types WHERE name_fr = %s"
    cursor.execute(query, (type_name,))
    result = cursor.fetchone()
    if result:
        type_id = result[0]
    else:
        insert_query = "INSERT INTO types (name_fr) VALUES (%s)"
        cursor.execute(insert_query, (type_name,))
        connection.commit()
        type_id = cursor.lastrowid
    cursor.close()
    return type_id

# Vérifier et insérer un utilisateur dans la table `users`
def insert_user_if_not_exists(data, connection):
    try:
        cursor = connection.cursor()

        check_query = "SELECT id FROM users WHERE last_name = %s"
        cursor.execute(check_query, (data['last_name'],))
        user = cursor.fetchone()

        if user is None:
            insert_query = """
                INSERT INTO users (email, last_name, phone, region_id, roles, sexe, attachment_structure_id, status_id)
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s)
            """
            cursor.execute(insert_query, (
                data['email'],
                data['last_name'],
                data['phone'] if data['phone'] else '',
                data['region_id'],
                json.dumps(["ROLE_USER"]),
                data['sexe'],
                data['attachment_structure_id'],
                data['status_id']
            ))
            connection.commit()
            user_id = cursor.lastrowid
        else:
            user_id = user[0]

        cursor.close()
        return user_id
    except Exception as e:
        print(f"Erreur lors de l'insertion de l'utilisateur: {e}")
        return None

# Insertion des données dans les autres tables selon le schéma
def insert_innovation(data, connection):
    try:
        cursor = connection.cursor()
        insert_query = """
            INSERT INTO innovations (title_fr, prototype, brevet, impact_fr, innovator_id, specialty_domain_id, activity_class_id, application_domain_id, region_id, type_id)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
        """
        cursor.execute(insert_query, (
            data['title_fr'] if data['title_fr'] else '',
            data['prototype'] if data['prototype'] is not None else 0,
            data['brevet'] if data['brevet'] is not None else 0,
            data['impact_fr'] if data['impact_fr'] else '',
            data['innovator_id'],
            data['specialty_domain_id'] if data['specialty_domain_id'] else None,
            data['activity_class_id'] if data['activity_class_id'] else None,
            data['application_domain_id'] if data['application_domain_id'] else None,
            data['region_id'],
            data['type_id']
        ))
        connection.commit()
        cursor.close()
    except Exception as e:
        print(f"Erreur lors de l'insertion de l'innovation: {e}")

# Main
def main(file_path):
    connection = connect_db()

    if connection is None:
        return

    df = load_excel_data(file_path)

    for index, row in df.iterrows():
        try:
            email = clean_email(row['email'], index + 1)
            phone = clean_phone(row['Contact téléphonique']) if pd.notna(row['Contact téléphonique']) else None
            sexe = clean_sexe(row['Sexe/Institut'])

            status_id = get_status_id(row['Statut'], connection) if pd.notna(row['Statut']) else None
            region_id = get_region_id(row['Region'], connection) if pd.notna(row['Region']) else None
            activity_class_id = get_activity_class_id(row['Classe d\'activité'], connection) if pd.notna(row['Classe d\'activité']) else None
            application_domain_id = get_application_domain_id(row['Domaine d\'application'], connection) if pd.notna(row['Domaine d\'application']) else None
            specialty_domain_id = get_specialty_domain_id(row['Domaine spécialité'], connection) if pd.notna(row['Domaine spécialité']) else None
            attachment_structure_id = get_attachment_structure_id(row['Structure de rattachement'], connection) if pd.notna(row['Structure de rattachement']) else None

            user_data = {
                'last_name': row['NOM INNOVATEUR'].split()[-1] if pd.notna(row['NOM INNOVATEUR']) else '',
                'email': email,
                'phone': phone,
                'region_id': region_id,
                'sexe': sexe,
                'attachment_structure_id': attachment_structure_id,
                'status_id': status_id
            }

            user_id = insert_user_if_not_exists(user_data, connection)

            innovation_data = {
                'title_fr': row['LIBELLE INNOVATION'] if pd.notna(row['LIBELLE INNOVATION']) else '',
                'prototype': 1 if row['Prototype'] == 'Oui' else 0,
                'brevet': 1 if row['Brevet'] == 'Oui' else 0,
                'impact_fr': row['Impact'] if pd.notna(row['Impact']) else '',
                'innovator_id': user_id,
                'specialty_domain_id': specialty_domain_id,
                'activity_class_id': activity_class_id,
                'application_domain_id': application_domain_id,
                'region_id': region_id,
                'type_id': get_type_id(row['Type (Procédé/Produit)'], connection) if pd.notna(row['Type (Procédé/Produit)']) else None
            }

            insert_innovation(innovation_data, connection)
        except Exception as e:
            print(f"Erreur lors du traitement de la ligne {index + 1}: {e}")

    connection.close()

if __name__ == '__main__':
    file_path = 'data/Données traitées sur les innovations.xlsx'
    main(file_path)