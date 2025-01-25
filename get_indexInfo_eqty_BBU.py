import os
import pathlib
import numpy as np
import pandas as pd

# Importiere die gekürzte local_utils, in der u.a. get_data, get_files, get_reference_data und get_historical_data
# definiert sind:
from local_utils import *

def get_equity_price_batch(tickers, check_date):
    """
    Holt PX_LAST für alle übergebenen Tickers an einem bestimmten Datum (check_date).
    Gibt ein Dict {Ticker: Preis} zurück.
    """
    try:
        historical_data = get_historical_data(
            securities=tickers,
            fields=["PX_LAST"],
            start_date=check_date,
            end_date=check_date
        )
        # 'historical_data' soll am Ende mindestens eine Spalte 'Security' und 'PX_LAST' enthalten
        price_dict = historical_data.set_index("Security")["PX_LAST"].to_dict()
        return {ticker: price_dict.get(ticker, np.nan) for ticker in tickers}
    except Exception as e:
        print(f"Error fetching historical equity prices: {e}")
        return {ticker: np.nan for ticker in tickers}

def main():
    # ------------------------------------------------------------------------
    # 1) Daten einlesen und vorbereiten
    # ------------------------------------------------------------------------
    PATH = pathlib.Path(os.path.abspath("Q:/"))
    INDEX_PATH = PATH / "ASMA" / "SYSTEME_DATEN" / "Bloomberg" / "Bloomberg SFTP" / "PM_TOOL"
    
    # Excel-Datei mit "UT_PM_LGCPTRUU.xls" im Namen suchen + einlesen
    index_file = get_files(INDEX_PATH, filter_key="UT_PM_LGCPTRUU.xls")
    index_data = get_data(
        filename=index_file[0],  # Wichtig: wir nutzen 'filename=' statt 'file_path='
        find_colname="ISIN",
        drop_disclaimer=True,
        drop_null_key="Des"
    )
    
    # Entferne doppelte Issuer-Einträge
    index_data_unique = index_data.drop_duplicates(subset="Issuer")

    # Extrahiere ISIN-Liste und formatiere für Bloomberg
    indexISIN = index_data_unique["ISIN"].tolist()
    indexISIN = [f"/isin/{isin}" for isin in indexISIN]

    # ------------------------------------------------------------------------
    # 2) Abfragen von Bloomberg-Daten
    # ------------------------------------------------------------------------
    # Felder, die wir für jeden Bond benötigen:
    auxFlds = ["BOND_TO_EQY_TICKER", "CRNCY", "EQY_FUND_CRNCY"]
    indexInfo = get_reference_data(indexISIN, auxFlds)

    # Filtere unvollständige Einträge
    filtered_indexInfo = indexInfo[
        indexInfo["BOND_TO_EQY_TICKER"].notna() & (indexInfo["BOND_TO_EQY_TICKER"] != "") &
        indexInfo["CRNCY"].notna() & (indexInfo["CRNCY"] != "") &
        indexInfo["EQY_FUND_CRNCY"].notna() & (indexInfo["EQY_FUND_CRNCY"] != "")
    ]

    # Gruppieren (z.B. nur die erste Zeile pro Ticker) und zurücksetzen des Index
    unique_indexInfo = (
        filtered_indexInfo
        .groupby(["BOND_TO_EQY_TICKER"], as_index=False)
        .first()
        .sort_values(by="BOND_TO_EQY_TICKER")
    )

    # ------------------------------------------------------------------------
    # 3) Company-Name-Step entfällt. (Wichtig: "Equity"-Suffix kommt gleich.)
    # ------------------------------------------------------------------------

    # ------------------------------------------------------------------------
    # 4) Abfrage von Aktienkursen
    # ------------------------------------------------------------------------
    check_date = "2024-09-03"
    tickers_with_equity_suffix = (unique_indexInfo["BOND_TO_EQY_TICKER"] + " Equity").tolist()

    # Hole Prices in einem Rutsch
    price_dict = get_equity_price_batch(tickers_with_equity_suffix, check_date)

    # Mappe PX_LAST auf das DataFrame
    unique_indexInfo["PX_LAST"] = (
        unique_indexInfo["BOND_TO_EQY_TICKER"] + " Equity"
    ).map(price_dict)

    # Nur Einträge mit gültigem Aktienkurs behalten (d.h. keine NaNs)
    equity_indexInfo = unique_indexInfo[unique_indexInfo["PX_LAST"].notna()]

    # ------------------------------------------------------------------------
    # 5) Finale Bearbeitung & Excel-Ausgabe (nur 3 Spalten)
    # ------------------------------------------------------------------------
    # Wir holen hier nur die Spalte, in die wir gleich " Equity" anhängen (BOND_TO_EQY_TICKER)
    final_df = equity_indexInfo[["BOND_TO_EQY_TICKER"]].copy()

    # (a) BOND_TO_EQY_TICKER -> BOND_TO_EQY_TICKER + " Equity"
    final_df["BOND_TO_EQY_TICKER"] = final_df["BOND_TO_EQY_TICKER"] + " Equity"

    # (b) Umbenennen zu "Security ID"
    final_df.rename(columns={"BOND_TO_EQY_TICKER": "Security ID"}, inplace=True)

    # (c) Zwei neue Spalten: Position (100) und Portfolio Name (LGCPTRUU_EQTY)
    final_df["Position"] = 100
    final_df["Portfolio Name"] = "LGCPTRUU_EQTY"

    # (d) Reihenfolge festlegen & Excel speichern
    final_df = final_df[["Security ID", "Position", "Portfolio Name"]]
    final_df.to_excel("final_indexInfo.xlsx", index=False)

    print("Ergebnis wurde in 'final_indexInfo.xlsx' gespeichert.")
    return final_df

if __name__ == "__main__":
    from time import time
    start_time = time()
    df_result = main()
    print(f"[TASK completed in {int(time() - start_time)} seconds]")
