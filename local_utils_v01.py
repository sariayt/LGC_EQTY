# local_utils.py (gekürzte Fassung)

import os
import pathlib
import warnings
warnings.filterwarnings("ignore", category=Warning)  # Optional: Warnungen unterdrücken

import pandas as pd
import numpy as np

# Bloomberg Imports
import blpapi

# =============================================================================
# 1) get_files
# =============================================================================
def get_files(folderpath, filter_key="LGXSTRUU", extensions=None):
    """
    Sucht in 'folderpath' nach Dateien mit 'filter_key' im Namen und bestimmten Endungen.
    Gibt eine Liste von Pfadobjekten zurück.
    """
    if extensions is None:
        extensions = [".xls", ".xlsx"]
    if isinstance(extensions, str):
        extensions = [extensions]
    for i, ext in enumerate(extensions):
        if not ext.startswith("."):
            extensions[i] = "." + ext
    
    files = []
    for file in folderpath.iterdir():
        # Überspringe Dateien ohne gewünschte Extension oder Dateien, die gerade geöffnet sind (~$)
        if (file.suffix not in extensions) or file.stem.startswith("~$"):
            continue

        if filter_key in file.name:
            files.append(file)
    return files

# =============================================================================
# 2) get_data
# =============================================================================
def get_data(filename, find_colname="ISIN", return_info=False,
             drop_disclaimer=False, drop_null_key=None, sheet=0):
    """
    Liest eine Excel-Datei ein, sucht 'find_colname' als Spaltenüberschrift,
    setzt ab dort die Spaltennamen und gibt ein DataFrame zurück.
    Optional: Disclaimer-Zeile löschen, Zeilen mit null in 'drop_null_key' entfernen usw.
    """
    # Laden
    df = pd.read_excel(filename, sheet_name=sheet, header=None)

    # Zeile finden, in der 'find_colname' steht
    for idx, row in df.iterrows():
        if find_colname in row.to_list():
            break

    # ggf. letzte Zeile (Disclaimer) entfernen
    if drop_disclaimer:
        data = df.iloc[idx+1:-1].copy().reset_index(drop=True)
    else:
        data = df.iloc[idx+1:].copy().reset_index(drop=True)

    # Spaltennamen zuweisen
    data.columns = row.to_list()

    # Leere letzte Zeilen entfernen
    while len(data) > 0:
        last_row = data.iloc[-1].values.flatten().tolist()
        # Wenn alles NaN ist, Zeile entfernen
        if all(pd.isna(x) for x in last_row):
            data = data.iloc[:-1].reset_index(drop=True)
        else:
            break

    # Spalten mit NaN als Name entfernen
    if np.nan in data.columns:
        data.drop(columns=np.nan, inplace=True)

    # Zeilen mit NaN in 'drop_null_key' entfernen
    if drop_null_key is not None:
        data = data[data[drop_null_key].notna()].reset_index(drop=True)

    # Whitespace in Spaltennamen trimmen
    data.columns = data.columns.str.strip()

    data = data.infer_objects()

    if not return_info:
        return data
    # Oder: (data, info) zurückgeben – falls man die Info-Zeilen braucht
    return data, None


# =============================================================================
# 3) Bloomberg Session-Management
# =============================================================================
def start_bloomberg_session():
    """
    Startet eine Bloomberg-API-Session.
    """
    session_options = blpapi.SessionOptions()
    session = blpapi.Session(session_options)
    
    if not session.start():
        raise ConnectionError("Bloomberg connection error!")
    return session

def stop_bloomberg_session(session):
    """
    Beendet eine Bloomberg-API-Session.
    """
    if not session.stop():
        raise ConnectionError("Bloomberg session didn't close correctly!")

# =============================================================================
# 4) get_reference_data
# =============================================================================
def get_reference_data(securities, fields, overrides=None):
    """
    Holt Stammdaten über die Bloomberg-API: z.B. BOND_TO_EQY_TICKER, CRNCY, usw.
    """
    if isinstance(securities, str):
        securities = [securities]
    if isinstance(fields, str):
        fields = [fields]

    session = start_bloomberg_session()

    session.openService("//blp/refdata")
    ref_service = session.getService("//blp/refdata")
    request = ref_service.createRequest("ReferenceDataRequest")

    # Tickers
    securities_element = request.getElement("securities")
    for ticker in securities:
        securities_element.appendValue(ticker)

    # Felder
    fields_element = request.getElement("fields")
    for f in fields:
        fields_element.appendValue(f)

    # Overrides, falls benötigt
    if overrides is not None:
        overridesElement = request.getElement("overrides")
        for fieldId, value in overrides.items():
            override = overridesElement.appendElement()
            override.setElement("fieldId", fieldId)
            override.setElement("value", value)

    session.sendRequest(request)

    data_records = []
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement("securityData"):
                securityElements = msg.getElement("securityData")
                for i in range(securityElements.numValues()):
                    securityValue = securityElements.getValue(i)
                    security = securityValue.getElement("security").getValueAsString()

                    fieldElements = securityValue.getElement("fieldData")
                    row = {"Security": security}
                    for j in range(fieldElements.numElements()):
                        fieldElement = fieldElements.getElement(j)
                        field_name = str(fieldElement.name())
                        field_val = fieldElement.getValue()
                        row[field_name] = field_val
                    data_records.append(row)

        if event.eventType() == blpapi.Event.RESPONSE:
            break

    stop_bloomberg_session(session)

    return pd.DataFrame(data_records)


# =============================================================================
# 5) get_historical_data
# =============================================================================
DEFAULT_HIST_REQUEST_OPTIONS = {
    "periodicityAdjustment": "ACTUAL",
    "periodicitySelection": "DAILY",
    "nonTradingDayFillOption": "ALL_CALENDAR_DAYS",
    "nonTradingDayFillMethod": "PREVIOUS_VALUE",
}

def get_historical_data(securities, fields, start_date, end_date):
    """
    Holt historische Tages-Daten (z.B. PX_LAST) für die angegebenen Ticker
    und Felder von start_date bis end_date.
    """
    if isinstance(securities, str):
        securities = [securities]
    if isinstance(fields, str):
        fields = [fields]

    # Start/End in Bloomberg-Format YYYYMMDD
    start_date = str(start_date).replace("-", "")
    end_date = str(end_date).replace("-", "")

    session = start_bloomberg_session()
    session.openService("//blp/refdata")
    ref_service = session.getService("//blp/refdata")
    request = ref_service.createRequest("HistoricalDataRequest")

    request.set("startDate", start_date)
    request.set("endDate", end_date)

    # Standard-Optionen setzen (Daily, Fülloptionen, usw.)
    for key, val in DEFAULT_HIST_REQUEST_OPTIONS.items():
        request.set(key, val)

    # Tickers
    securities_element = request.getElement("securities")
    for ticker in securities:
        securities_element.appendValue(ticker)

    # Felder
    fields_element = request.getElement("fields")
    for f in fields:
        fields_element.appendValue(f)

    session.sendRequest(request)

    data_records = []
    while True:
        ev = session.nextEvent()
        for msg in ev:
            if msg.hasElement("securityData"):
                securityData = msg.getElement("securityData")
                securityName = securityData.getElementAsString("security")
                fieldDataArray = securityData.getElement("fieldData")

                for i in range(fieldDataArray.numValues()):
                    fieldData = fieldDataArray.getValueAsElement(i)
                    date_val = str(fieldData.getElementAsDatetime("date"))
                    row = {"Security": securityName, "Date": date_val}
                    for field in fields:
                        if fieldData.hasElement(field):
                            row[field] = fieldData.getElementValue(field)
                        else:
                            row[field] = np.nan
                    data_records.append(row)

        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    stop_bloomberg_session(session)

    return pd.DataFrame(data_records)
