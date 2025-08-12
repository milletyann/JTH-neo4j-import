def read_instance_credentials(db_loc):
    creds = {}
    path = f"credentials_{db_loc}.txt"

    with open(path, "r", encoding="utf-8") as f:
        for line in f:
            # Ignore commentaires ou lignes vides
            line = line.strip()
            if not line or line.startswith("#"):
                continue

            # Split sur le premier signe égal
            if "=" in line:
                key, value = line.split("=", 1)
                creds[key.strip()] = value.strip()

    return creds
