import pandas as pd
import json
import logging
from pathlib import Path

# 1. Logging-Konfiguration (Protokollierung der Schritte)
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s',
    handlers=[
        logging.FileHandler("bereinigung_log.log", encoding='utf-8'),
        logging.StreamHandler()
    ]
)


class DatenBereiniger:
    """Klasse zur automatisierten Datenbereinigung basierend auf einer JSON-Konfiguration."""

    def __init__(self, konfigurations_datei: str):
        # Verwendung von Pathlib für flexible Pfadverwaltung
        self.basis_pfad = Path(__file__).parent.parent
        self.konfig_pfad = self.basis_pfad / "config" / konfigurations_datei
        self.konfiguration = self.lade_konfiguration()
        self.df = None

    def lade_konfiguration(self):
        """Lädt die Einstellungen aus der JSON-Datei."""
        try:
            with open(self.konfig_pfad, 'r', encoding='utf-8') as f:
                logging.info(f"Konfiguration geladen von: {self.konfig_pfad}")
                return json.load(f)
        except Exception as e:
            logging.error(f"Fehler beim Laden der Konfiguration: {e}")
            return None

    def lade_daten(self, eingabe_datei: str):
        """Liest die CSV-Daten ein."""
        daten_pfad = self.basis_pfad / "data" / eingabe_datei
        try:
            self.df = pd.read_csv(daten_pfad)
            logging.info(f"Daten erfolgreich geladen. Anzahl der Zeilen: {len(self.df)}")
        except Exception as e:
            logging.error(f"Fehler beim Laden der Daten: {e}")

    def entferne_fehlwerte(self):
        """Löscht Zeilen mit fehlenden Werten (Drop NA)."""
        if self.konfiguration.get("drop_na"):
            vorher = self.df.shape[0]
            self.df = self.df.dropna()
            logging.info(f"Fehlwerte entfernt. Zeilen vorher: {vorher}, nachher: {self.df.shape[0]}")

    def entferne_duplikate(self):
        """Löscht doppelte Datensätze (Drop Duplicates)."""
        if self.konfiguration.get("drop_duplicates"):
            vorher = self.df.shape[0]
            self.df = self.df.drop_duplicates()
            logging.info(f"Duplikate entfernt. Zeilen vorher: {vorher}, nachher: {self.df.shape[0]}")

    def korrigiere_datentypen(self):
        """Korrigiert Datentypen basierend auf der Konfiguration."""
        korrekturen = self.konfiguration.get("data_type_corrections", {})
        for spalte, datentyp in korrekturen.items():
            if spalte in self.df.columns:
                try:
                    self.df[spalte] = self.df[spalte].astype(datentyp)
                    logging.info(f"Datentyp für Spalte '{spalte}' in {datentyp} geändert.")
                except Exception as e:
                    logging.warning(f"Korrektur für Spalte '{spalte}' fehlgeschlagen: {e}")

    def entferne_spalten(self):
        """Entfernt nicht benötigte Spalten."""
        zu_entfernende_spalten = self.konfiguration.get("remove_columns", [])
        vorhandene_spalten = [s for s in zu_entfernende_spalten if s in self.df.columns]
        if vorhandene_spalten:
            self.df = self.df.drop(columns=vorhandene_spalten)
            logging.info(f"Folgende Spalten wurden entfernt: {vorhandene_spalten}")

    def speichere_daten(self, ausgabe_datei: str):
        """Speichert die bereinigten Daten in eine neue CSV-Datei."""
        ausgabe_pfad = self.basis_pfad / "data" / ausgabe_datei
        try:
            self.df.to_csv(ausgabe_pfad, index=False)
            logging.info(f"Bereinigte Daten gespeichert unter: {ausgabe_pfad}")
        except Exception as e:
            logging.error(f"Fehler beim Speichern der Daten: {e}")

    def ausfuehren(self, eingabe: str, ausgabe: str):
        """Startet den gesamten Bereinigungsprozess (Pipeline)."""
        logging.info("Start der Datenbereinigung...")
        self.lade_daten(eingabe)

        if self.df is not None:
            self.entferne_fehlwerte()
            self.entferne_duplikate()
            self.korrigiere_datentypen()
            self.entferne_spalten()
            self.speichere_daten(ausgabe)
            logging.info("Datenbereinigung erfolgreich abgeschlossen.")


# Programmstart
if __name__ == "__main__":
    bereiniger = DatenBereiniger("cleaning_config.json")
