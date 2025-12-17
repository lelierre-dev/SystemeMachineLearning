from feast import Entity

# Définition de l'entité principale "user"
user = Entity(
    name="user",                    # Nom logique de l'entité
    join_keys=["user_id"],          # Clé de jointure correspondant à la colonne Postgres
    description="Utilisateur de la plateforme StreamFlow"
)