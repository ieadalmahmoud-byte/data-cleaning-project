import pandas as pd
import json
import os


def clean_data():
    # 1. Pfade definieren
    config_path = os.path.join('..', 'config', 'cleaning_config.json')
    data_path = os.path.join('..', 'data', 'my_data.csv')
    output_path = os.path.join('..', 'data', 'bereinigte_daten.csv')

    # 2. Konfigurationsdatei laden
    with open(config_path, 'r', encoding='utf-8') as f:
        config = json.load(f)

    # 3. Datendatei einlesen
    df = pd.read_csv(data_path)
    print("Daten wurden erfolgreich geladen.")

    # 4. Bereinigungsschritte basierend auf der Konfiguration ausführen (laut Notes.md)

    # a. Fehlende Werte entfernen (Drop NA)
    if config.get("drop_na"):
        df = df.dropna()
        print("- Fehlende Werte wurden entfernt.")

    # b. Duplikate entfernen (Drop Duplicate)
    if config.get("drop_duplicates"):
        df = df.drop_duplicates()
        print("- Duplikate wurden entfernt.")

    # c. Datentypen korrigieren (Data Type Correction)
    if config.get("data_type_correction"):
        for col, dtype in config["data_type_correction"].items():
            if col in df.columns:
                df[col] = df[col].astype(dtype)
        print("- Datentypen wurden korrigiert.")

    # d. Bestimmte Spalten entfernen (Remove certain columns)
    if config.get("columns_to_remove"):
        df = df.drop(columns=config["columns_to_remove"], errors='ignore')
        print(f"- Folgende Spalten wurden entfernt: {config['columns_to_remove']}")

    # 5. Bereinigte Daten speichern
    df.to_csv(output_path, index=False)
    print(f"\nErfolg! Die bereinigte Datei wurde hier gespeichert: {output_path}")


if __name__ == "__main__":
    clean_data()

