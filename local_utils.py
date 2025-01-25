
# General packages
import os
import pathlib
import re
import time
import datetime

from io import BytesIO, StringIO

import pandas as pd
import numpy as np
from scipy import stats, interpolate

# Ignore annoing warnings by pandas
import warnings
warnings.filterwarnings("ignore", category=Warning)



# # =============================================================================
# # FUNCTIONS
# # =============================================================================


def get_file_date(filepath):
    """List of datetime objects from the file name."""
    if isinstance(filepath, pathlib.Path):
        filename = filepath.stem

        date_match = re.search(r"\d{8}$", filename)  # Match format: YYYYMMDD
        if date_match:
            return pd.to_datetime(date_match.group(0))

        date_match = re.search(r"\d{4}_\d{2}$", filename)  # Match format: YYYY_MM
        if date_match:
            year_month = date_match.group(0)
            year, month = year_month.split("_")
            return pd.to_datetime(f"{year}-{month}")

        raise ValueError("Invalid date format in the file name.")

    elif isinstance(filepath, (list, tuple)):
        result = []
        for file in filepath:
            result.append(get_file_date(file))
        return result

    raise ValueError("Argument must be a valid a pathlib.Path object or list of paths.")


def get_files(folderpath, filter_key="LGXSTRUU", extensions=None):

    # Check extensions formating is a list with the dot prefix
    if extensions is None:
        extensions = [".xls", ".xlsx"]
    if isinstance(extensions, str):
        extensions = [extensions]
    for i, ext in enumerate(extensions):
        if not ext.startswith("."):
           extensions[i] = "." + ext
    
    # List of files filtered by key
    files = []
    for file in folderpath.iterdir():
        if (file.suffix not in extensions) or (file.stem.startswith('~$')):
            # Skip files without specified extension and opened files (start with ~$)
            continue

        if filter_key in file.name:
            files.append(file)
    return files


def get_latest_files(folderpath, filter_keys, extensions=None):
    """Latest files in the directory."""

    if not isinstance(filter_keys, list):
        filter_keys = [filter_keys]

    latest_dates = []
    latest_files = []
    for filter_key in filter_keys:
        # Get list of files
        files = get_files(folderpath, filter_key=filter_key, extensions=extensions)

        # Get file timestamp
        timestamp = get_file_date(files)
        
        # Get latest
        latest_idx = np.argmax(timestamp)

        latest_files.append(files[latest_idx])
        latest_dates.append(timestamp[latest_idx])

    if not all(date == latest_dates[0] for date in latest_dates):
        print(f"Warning! Latest dates are different {latest_dates}.")
    
    if len(filter_keys) == 1:
        return latest_files[0]
    return latest_files


def get_data(filename, find_colname="ISIN", return_info=False, drop_disclaimer=False, drop_null_key=None, sheet=0):
    """Get data from excel file."""
    
    if isinstance(filename, pathlib.Path):
        print(f"Loading data... {filename.parent.name}/{filename.name}")

    df = pd.read_excel(filename, sheet_name=sheet, header=None)

    # Find row with data column names, uses find_colname
    for idx, row in df.iterrows():
        if find_colname in row.to_list():
            break

    # Drop disclaimer of last row
    if drop_disclaimer:
        data = df.iloc[idx+1:-1].copy().reset_index(drop=True)
    else:
        data = df.iloc[idx+1:].copy().reset_index(drop=True)

    # Insert column names
    data.columns = row.to_list()

    # Drop any empty last rows
    for i in range(1, len(df)):
        last_row = data.iloc[-1].values.flatten().tolist()
        try:
            if all(np.isnan(last_row)):
                data = data.iloc[:-1].copy().reset_index(drop=True)
        except TypeError as error:
            break

    # Drop any empty column name (np.nan as name)
    if np.nan in data.columns:
        data.drop(columns=np.nan, inplace=True)

    # Remove rows without `drop_null_key`
    if drop_null_key is not None:
        null_key = data[drop_null_key].isnull()
        if null_key.sum() >= 1:
            # print(f"Dropping {null_key.sum()} fields with null {drop_null_key}.")
            data = data[~null_key].reset_index(drop=True)

    # Remove spaces at the beginning and end of each column name
    data.columns = data.columns.str.strip()
    # Change to appropiate dtypes
    data = data.infer_objects()

    # Return only the data if specified
    if not return_info:
        return data

    info = df.iloc[:idx,0].dropna().values.flatten().tolist()
    return data, info


# ============================================================================
# Copyright (c) 2023, Aytekin Sari. All rights reserved.
# blprequest.py
# ============================================================================


import blpapi
import pandas as pd
import numpy as np
from datetime import datetime, timedelta

def start_bloomberg_session():
    """
    Start a Bloomberg API session.

    This function initializes and starts a session for Bloomberg API. It sets up the session 
    options and attempts to start the session. If the session fails to start, it raises a 
    ConnectionError.

    Returns
    -------
    session : blpapi.Session
        The Bloomberg API session object that has been started.

    Raises
    ------
    ConnectionError
        If the Bloomberg session fails to start.

    Examples
    --------
    >>> session = start_bloomberg_session()
    """
    session_options = blpapi.SessionOptions()
    session = blpapi.Session(session_options)
    
    session_status = session.start()
    if not session_status:
        raise ConnectionError('Bloomberg connection error!')

    return session

def stop_bloomberg_session(session):
    """
    Stop a Bloomberg API session.

    This function stops an active Bloomberg API session. If the session fails to stop 
    properly, it raises a ConnectionError.

    Parameters
    ----------
    session : blpapi.Session
        The Bloomberg API session object to be stopped.

    Raises
    ------
    ConnectionError
        If there is an issue closing the Bloomberg session.

    Examples
    --------
    >>> stop_bloomberg_session(session)
    """
    session_status = session.stop()

    if not session_status:
        raise ConnectionError("Bloomberg session didn't close correctly!")


def get_reference_data(securities, fields, overrides=None):
    """
    Retrieve reference data for specified securities and fields from Bloomberg.

    This function connects to the Bloomberg API and sends a request for reference data 
    for a list of securities and fields. It supports the use of overrides to specify 
    additional parameters for the query. The function then processes the received data 
    and returns it in a structured format.

    Parameters
    ----------
    securities : str or list of str
        The ticker(s) or identifier(s) of the securities to query.
    fields : str or list of str
        The field(s) for which data is requested.
    overrides : dict, optional
        A dictionary of overrides to apply to the request. Each key-value pair in the 
        dictionary represents a field to override and its desired value. Defaults to None.

    Returns
    -------
    DataFrame
        A pandas DataFrame containing the requested data. Each row represents data for one 
        security, and columns correspond to the requested fields and security identifier.

    Raises
    ------
    ConnectionError
        If there are issues with starting or stopping the Bloomberg session.

    Examples
    --------
    >>> securities = ['AAPL US Equity', 'MSFT US Equity']
    >>> fields = ['PX_LAST', 'PX_VOLUME']
    >>> data = get_reference_data(securities, fields)
    """
    # Make securities and fields lists    
    if isinstance(securities, str): securities = [securities]
    if isinstance(fields, str): fields = [fields]

    # New session   
    session = start_bloomberg_session()

    # Create request
    session.openService("//blp/refdata")
    ref_service = session.getService("//blp/refdata")
    request = ref_service.createRequest("ReferenceDataRequest")

    # Set securities and fields in the request    
    securities_element = request.getElement("securities")
    [securities_element.appendValue(ticker) for ticker in securities]

    fields_element = request.getElement("fields")
    [fields_element.appendValue(field) for field in fields]

    if overrides is not None:
        # Create the 'overridesElement' in the request        
        overridesElement = request.getElement("overrides")

        # Loop through the 'overrides' dict and set each override        
        for fieldId, value in overrides.items():
            override = overridesElement.appendElement()
            override.setElement("fieldId", fieldId)
            override.setElement("value", value)      

    # Send the request
    session.sendRequest(request)

    # Process received events
    data_records = []
    while True:
        event = session.nextEvent()
        for msg in event:
            if msg.hasElement('securityData'):
                securityElements = msg.getElement("securityData")
                for i in range(securityElements.numValues()):
                    securityValue = securityElements.getValue(i)
                    security = securityValue.getElement("security").getValueAsString()

                    fieldElements = securityValue.getElement("fieldData")
                    field_values = {'Security': security}
                    for j in range(fieldElements.numElements()):
                        fieldElement = fieldElements.getElement(j)
                        field = str(fieldElement.name())
                        value = fieldElement.getValue()
                        field_values[field] = value

                    data_records.append(field_values)

        if event.eventType() == blpapi.Event.RESPONSE: 
            break

    # Close connection
    stop_bloomberg_session(session)

    # return data_records
    return pd.DataFrame.from_records(data_records)


DEFAULT_HIST_REQUEST_OPTIONS = {
    "periodicityAdjustment": "ACTUAL",
    "periodicitySelection": "DAILY",
    "nonTradingDayFillOption": "ALL_CALENDAR_DAYS",
    "nonTradingDayFillMethod": "PREVIOUS_VALUE",
    }

def get_historical_data(securities, fields, start_date, end_date, requestOptions=None):
    """
    Retrieve historical data for specified securities and fields from Bloomberg.

    This function connects to the Bloomberg API and sends a request for historical data 
    for a list of securities and fields, within a specified date range. It supports 
    additional request options to fine-tune the query. The function processes the received 
    data and returns it in a structured format.

    Parameters
    ----------
    securities : str or list of str
        The ticker(s) or identifier(s) of the securities for which historical data is requested.
    fields : str or list of str
        The field(s) for which historical data is requested.
    start_date : str or datetime-like
        The start date of the historical data range in 'YYYY-MM-DD' format or as a datetime object.
    end_date : str or datetime-like
        The end date of the historical data range in 'YYYY-MM-DD' format or as a datetime object.
    requestOptions : dict, optional
        A dictionary of additional request options. Each key-value pair in the dictionary 
        represents a specific option for the historical data request. Defaults to None.

    Returns
    -------
    DataFrame
        A pandas DataFrame containing the requested historical data. Each row represents 
        data for one security on a specific date, and columns correspond to the requested fields, 
        security identifier, and date.

    Raises
    ------
    ConnectionError
        If there are issues with starting or stopping the Bloomberg session.

    Examples
    --------
    >>> securities = ['AAPL US Equity']
    >>> fields = ['PX_LAST', 'PX_VOLUME']
    >>> start_date = '2023-01-01'
    >>> end_date = '2023-06-30'
    >>> data = get_historical_data(securities, fields, start_date, end_date)
    """   
    # Check if default values need to be updated
    if requestOptions is None:
        requestOptions = dict(DEFAULT_HIST_REQUEST_OPTIONS)
    else:
        requestOptions = {**DEFAULT_HIST_REQUEST_OPTIONS, **requestOptions}

    # Make securities and fields lists    
    if isinstance(securities, str): securities = [securities]
    if isinstance(fields, str): fields = [fields]

    start_date = str(start_date).replace('-', '')
    end_date = str(end_date).replace('-', '')

    # New session
    session = start_bloomberg_session()

    # Create request
    session.openService("//blp/refdata")
    ref_service = session.getService("//blp/refdata")
    request = ref_service.createRequest("HistoricalDataRequest")

    request.set("startDate", start_date)
    request.set("endDate", end_date)

    # Additional options
    [request.set(fieldId, value) for fieldId, value in requestOptions.items()]

    # Set securities and fields in the request
    securities_element = request.getElement("securities")
    [securities_element.appendValue(ticker) for ticker in securities]

    fields_element = request.getElement("fields")
    [fields_element.appendValue(field) for field in fields]

    # Send the request
    session.sendRequest(request)

    data_records = []

    # Process received events
    while True:
        ev = session.nextEvent()
        for msg in ev:
            if msg.hasElement("securityData"):
                securityData = msg.getElement("securityData")
                securityName = securityData.getElementAsString("security")

                fieldDataArray = securityData.getElement('fieldData')
                for i in range(fieldDataArray.numValues()):
                    fieldData = fieldDataArray.getValueAsElement(i)
                    date = str(fieldData.getElementAsDatetime("date"))

                    field_values = {}
                    for field in fields:
                        if fieldData.hasElement(field):
                            fieldValue = fieldData.getElementValue(field)
                            field_values[field] = fieldValue
                        else:
                            field_values[field] = np.nan

                    # Append to records
                    data_records.append({
                        'Security': securityName,
                        'Date': date,
                        **field_values
                         })

        if ev.eventType() == blpapi.Event.RESPONSE:
            break

    # Close connection
    stop_bloomberg_session(session)

    # return data_records
    return pd.DataFrame.from_records(data_records)


def isFieldValid(fields):
    """
    Check the validity of field names against Bloomberg's database.

    This function connects to the Bloomberg API and verifies if the provided field names 
    are valid Bloomberg field mnemonics. It queries the Bloomberg Field Information Service 
    for each field name and determines whether it exists in the Bloomberg database.

    Parameters
    ----------
    fields : list of str
        A list of field names (mnemonics) to be checked for validity.

    Returns
    -------
    DataFrame
        A pandas DataFrame containing the results of the validity check. Each row represents 
        one field name, with columns for the field name and a boolean indicating its validity.

    Raises
    ------
    ConnectionError
        If there are issues with starting or stopping the Bloomberg session.

    Examples
    --------
    >>> fields = ['PX_LAST', 'INVALID_FIELD']
    >>> valid_fields = isFieldValid(fields)
    """
    # New session    
    session = start_bloomberg_session()

    session.openService("//blp/apiflds")
    fieldInfoService = session.getService("//blp/apiflds")

    data_records = []

    for fieldName in fields:

        request = fieldInfoService.createRequest("FieldSearchRequest")
        request.set("searchSpec", fieldName)
        session.sendRequest(request)

        check = []

        while True:
            ev = session.nextEvent()
            for msg in ev:
                if msg.messageType()  == "fieldResponse": 
                    fieldDataArray = msg.getElement('fieldData')
                    for i in range(fieldDataArray.numValues()):
                        fieldData = fieldDataArray.getValueAsElement(i)
                        fieldInfo = fieldData.getElement('fieldInfo')
                        mnemonic = fieldInfo.getElementAsString("mnemonic")
                        if mnemonic == fieldName:
                            #check.append(mnemonic)      RERMARK: If we leave that and comment line 40, we get the fieldName (resp. mnemonic) if the fieldName exists, SEE OUTPUT2
                            #check.append(fieldData.getElementAsString("id")) #SEE OUTPUT1, here we get the id
                            check.append(True)
                        else:
                            check.append(False)

            if ev.eventType() == blpapi.Event.RESPONSE:
                    break
            
        data_records.append({
            'fieldName': fieldName,
            'isValid': any(check)})

    # Close connection
    stop_bloomberg_session(session)

    # return data_records
    return pd.DataFrame.from_records(data_records)


def get_index_data(index_ticker, field, start_date, end_date):
    """
    Retrieve data for a specified index and field over a given date range from Bloomberg.

    This function connects to Bloomberg and requests data for a specific index and field 
    for each day within the specified date range. It iterates through each date, making 
    individual requests, and compiles the results into a structured format.

    Parameters
    ----------
    index_ticker : str
        The ticker or identifier of the index for which data is requested.
    field : str
        The Bloomberg field name for which data is requested.
    start_date : str or datetime
        The start date of the data retrieval period in 'YYYY-MM-DD' format or as a datetime object.
    end_date : str or datetime
        The end date of the data retrieval period in 'YYYY-MM-DD' format or as a datetime object.

    Returns
    -------
    DataFrame
        A pandas DataFrame containing the requested data. Each row represents data for the index 
        on a specific date, and columns correspond to the requested field, date, and any additional 
        data returned by Bloomberg.

    Raises
    ------
    ConnectionError
        If there are issues with starting or stopping the Bloomberg session.

    Examples
    --------
    >>> index_ticker = 'SPX Index'
    >>> field = 'PX_LAST'
    >>> start_date = '2023-01-01'
    >>> end_date = '2023-01-31'
    >>> index_data = get_index_data(index_ticker, field, start_date, end_date)
    """
    # Check date type
    if not isinstance(start_date, datetime):
        start_date = datetime.fromisoformat(start_date)
    if not isinstance(end_date, datetime):
        end_date = datetime.fromisoformat(end_date)

    # New session
    session = start_bloomberg_session()

    session.openService("//blp/refdata")

    # Obtain the reference data service
    refDataService = session.getService("//blp/refdata")

    data_records = []

    # Loop through each date in the range
    date = start_date
    while date <= end_date:
        # Create and fill the request for the reference data
        request = refDataService.createRequest("ReferenceDataRequest")
        request.getElement("securities").appendValue(index_ticker)
        request.getElement("fields").appendValue(field)
 
        overrides = request.getElement("overrides")
        override1 = overrides.appendElement()
        override1.setElement("fieldId", "END_DT")
        override1.setElement("value", date.strftime('%Y%m%d'))

        # Send the request
        session.sendRequest(request)

        # Process received events
        while(True):
            # We provide timeout to give the chance for Ctrl+C handling:
            ev = session.nextEvent(500)
            for msg in ev:

                if msg.hasElement("securityData"):
                    securityDataArray = msg.getElement("securityData")

                    for i in range(securityDataArray.numValues()):
                        securityData = securityDataArray.getValueAsElement(i)
                        
                        if securityData.hasElement('fieldData') and securityData.getElement('fieldData').hasElement(field):
                            fieldData = securityData.getElement('fieldData').getElement(field)

                            for i in range(fieldData.numValues()):
                                fieldDataElement = fieldData.getValueAsElement(i)
                                record = {'Date': date.strftime('%Y-%m-%d')}
                                for j in range(fieldDataElement.numElements()):
                                    field_name = str(fieldDataElement.getElement(j).name())
                                    field_value = fieldDataElement.getElement(j).getValue()
                                    record[field_name] = field_value
                                data_records.append(record)

            if ev.eventType() == blpapi.Event.RESPONSE:
                # Response completely received, so we could exit
                break
        # Move to the next date
        date += timedelta(days=1)
    
    #Close conneciton    
    stop_bloomberg_session(session)    

    # return data_records
    return pd.DataFrame.from_records(data_records)


# ============================================================================
# Copyright (c) 2023, Aytekin Sari & Berner Kantonalbank. All rights reserved.
# input_parameters.py
# ============================================================================


from datetime import datetime, date
import pandas as pd

import pandas_market_calendars as mcal

import os
import pathlib


# ============================================================================
# Functions
# ============================================================================



def date_ISOformat(value):
    """
    Convert a date value to ISO 8601 format string.

    This function converts a date, either provided as a string or a Python
    date object, to an ISO 8601 formatted string ('YYYY-MM-DD').

    Parameters
    ----------
    value : str or datetime.date
        The date to be converted, either as a string in 'YYYY-MM-DD' format or
        as a datetime.date object.

    Returns
    -------
    str
        The date in ISO 8601 format string ('YYYY-MM-DD').

    Raises
    ------
    TypeError
        If the input 'value' is not a string or datetime.date object.

    Examples
    --------
    >>> date_ISOformat('2023-01-01')
    '2023-01-01'
    >>> date_ISOformat(datetime.date(2023, 1, 1))
    '2023-01-01'
    """
    if isinstance(value, str):
        return date.fromisoformat(value).isoformat()
    elif isinstance(value, date):
        return value.isoformat()
    raise TypeError("Property value must be of the format: 'yyyy-mm-dd'.")

def previous_busdate(calendar_name='NYSE'):
    """
    Determine the previous business day using a specified market calendar.

    This function calculates the previous business day based on a given market
    calendar. By default, it uses the New York Stock Exchange (NYSE) calendar.

    Parameters
    ----------
    calendar_name : str, optional
        The name of the market calendar to use. Default is 'NYSE'.

    Returns
    -------
    datetime.date
        The date of the previous business day.

    Examples
    --------
    >>> previous_busdate()
    datetime.date(2023, 11, 28)  # Assuming today is 2023-11-29

    >>> previous_busdate('LSE')
    datetime.date(2023, 11, 28)  # Assuming today is 2023-11-29 and using London Stock Exchange calendar
    """
    # Get the calendar
    calendar = mcal.get_calendar(calendar_name)
    
    # Get the schedule for the current year
    schedule = calendar.schedule(start_date=pd.Timestamp.today().replace(month=1, day=1),
                             end_date=pd.Timestamp.today().replace(month=12, day=31))
    
    # Extract the market close dates (which are the business days)
    business_days = schedule.index.date
    
    # Get the previous business day
    today = pd.Timestamp.today().date()
    prev_bday = max([day for day in business_days if day < today])
    
    return prev_bday


# ============================================================================
# Copyright (c) 2023, Aytekin Sari & Berner Kantonalbank. All rights reserved.
# getBbgFieldData
# ============================================================================

import os
import warnings
import numpy as np
import pandas as pd

from time import time
from datetime import datetime, timedelta

import blpapi

# Local imports
# from .io import read_hdf5, save_hdf5
# from .momentum import Momentum
# from .value import Value
# from .quality import Quality
# from .growth import Growth
# from .blprequest import isFieldValid, get_historical_data


def flatten_list(nested_list):
    """
    Flatten a nested list into a single-level list.

    This function recursively traverses each element in a nested list (a list of lists) 
    and flattens it into a single-level list. It handles multiple levels of nesting, 
    ensuring that all nested elements are extracted and placed in a single, flat list.

    Parameters
    ----------
    nested_list : list
        A list which may contain nested lists (sublists) at any depth.

    Returns
    -------
    list
        A flat list containing all elements from the nested list, with the same 
        ordering preserved.

    Examples
    --------
    >>> nested_list = [1, [2, [3, 4], 5], 6]
    >>> flat_list = flatten_list(nested_list)
    >>> print(flat_list)
    [1, 2, 3, 4, 5, 6]
    """
    flattened_list = []
    for item in nested_list:
        if isinstance(item, list):
            flattened_list.extend(flatten_list(item))
        else:
            flattened_list.append(item)
    return flattened_list

def structureFieldData(data, security_name='Security', date_name='Date'):
    """
    Construct a 3D array from structured financial data.

    This function transforms a DataFrame containing financial data into a 3D NumPy array. 
    The data is restructured such that each 'slice' of the array corresponds to a field, 
    with dimensions representing dates, securities, and field values. The function also 
    returns lists of unique securities, dates, and field names for reference.

    Parameters
    ----------
    data : DataFrame
        A pandas DataFrame containing financial data with columns for securities, dates, and various fields.
    security_name : str, optional
        The name of the column in 'data' representing security identifiers. Default is 'Security'.
    date_name : str, optional
        The name of the column in 'data' representing dates. Default is 'Date'.

    Returns
    -------
    dict
        A dictionary containing the structured data with keys:
        - 'fieldData': A 3D NumPy array with shape (nDates, nSecurities, nFields).
        - 'securities': A list of unique securities present in the data.
        - 'dates': A list of unique dates present in the data.
        - 'fieldNames': A list of field names (excluding security and date names).

    Notes
    -----
    - The function assumes the input DataFrame has a consistent structure with identifiable 
      columns for security names and dates.
    - The 3D array is structured such that each 'slice' along the third dimension corresponds 
      to a different field.

    Examples
    --------
    >>> data = pd.DataFrame({'Security': ['AAPL', 'MSFT'], 'Date': ['2020-01-01', '2020-01-02'], 'PX_LAST': [300, 200]})
    >>> structured_data = structureFieldData(data)
    """

    # Sort the DataFrame by Security and Date to make sure the data is in the right order
    df = data.sort_values(by=[security_name, date_name])
    
    # Filter the field columns (excluding security_name and date_name)
    field_columns = df.columns.difference([security_name, date_name], sort=True).to_list()

    # Get the DataFrame with sorted fields, while keeping security and date columns first
    sorted_cols = [security_name, date_name, *field_columns]
    df = df[sorted_cols]

    # Extract unique Security and Date values and sort them
    unique_securities = np.sort(df[security_name].unique()).tolist()
    unique_dates = np.sort(df[date_name].unique()).tolist()

    # Create an empty 3D NumPy array
    nSecurities = len(unique_securities)
    nDates = len(unique_dates)
    nFields = len(field_columns)
    result_array = np.empty((nDates, nSecurities, nFields))

    # Here we work directly with numpy arrays for speed up
    flat_array = df.to_numpy()                  # Get the numpy array
    column_names = df.columns.to_list()
    
    # Get the numpy index locations of security_name and date_name
    security_idx = column_names.index(security_name)
    date_idx = column_names.index(date_name)

    # Get the numpy index location of the remaining columns, i.e. the Fields
    field_columns_idx = [idx for idx in range(len(column_names)) if idx not in [security_idx, date_idx]]

    # Populate the 3D array with data from the DataFrame
    for i, security in enumerate(unique_securities):
        security_window = flat_array[:, security_idx] == security
        security_data = flat_array[security_window]

        for j, date in enumerate(unique_dates):
            date_window = security_data[:, date_idx] == date
            date_data = security_data[date_window]
            row_data = date_data[:, field_columns_idx]

            # Check if there are multiple rows with same security and value
            if row_data.ndim == 2:
                # Combine dropping the nan
                result_array[j, i, :] = np.nanmin(row_data, axis=0)
                #result_array[j, i, :] = np.nanmax(row_data, axis=0)
            else:
                result_array[j, i, :] = row_data

    result = {
        "fieldData": result_array,
        "securities": unique_securities,
        "dates": unique_dates,
        "fieldNames": field_columns
    }
    return result


def getBbgFieldData(specs, indexData, fieldNames, overrideFields=None, session=None):
    """
    Load instrument data from Bloomberg based on specified parameters.

    This function retrieves financial data from Bloomberg for instruments listed in 
    'indexData' and for fields specified in an Excel file referenced in 'specs'. 
    The data is loaded in multiple batches to enhance reliability and performance, 
    and to manage Bloomberg constraints. If the daily data limit is likely to be 
    reached, the loading of subsequent batches is delayed until the next day. After 
    downloading, the batches are merged and synchronized across all fields.

    Parameters
    ----------
    specs : object
        An object containing specifications such as index ticker, start/end dates, 
        periodicity, file name for fields, index type, etc.
    indexData : dict
        A dictionary containing information about the instruments that belonged to 
        the index in the specified period.
    session : blpapi.Session, optional
        An optional Bloomberg connection object. If not provided, a new session is 
        created. Not implemented.

    Returns
    -------
    dict
        A dictionary containing structured Bloomberg field data, including observation 
        dates, field names, field data, index constituents, and field types.

    Notes
    -----
    - The function handles downloading data in batches and applies any necessary 
      overrides specified in the Excel file.
    - Temporary files are created for each batch and overrides, and merged subsequently.
    - Fields are checked for validity before data retrieval.

    Examples
    --------
    >>> specs = InputParameters(...)
    >>> indexData = {'instr': ['AAPL US Equity', 'MSFT US Equity'], ...}
    >>> bbgFieldData = getBbgFieldData(specs, indexData)
    """

    allTicker = sorted([f"{instr} {specs['indexType']}" for instr in np.unique(indexData['instr'])])
    nAssets = len(allTicker)
    nFields = len(fieldNames)

    # Prepare a dictionary for quick lookup of override fields
    overrideDict = {item['fieldName']: item['overrides'] for item in overrideFields} if overrideFields else {}

    print(f" [{datetime.now()}] Downloading field data for entire universe.")

    # Calculate number of batches based on maximum hits, maximum size, etc.
    maxHits = 0.9 * 500000
    startDate = datetime.strptime(specs['startDate'], '%Y-%m-%d')
    endDate = datetime.strptime(specs['endDate'], '%Y-%m-%d')
    dates =  [startDate + timedelta(x) for x in range((endDate - startDate).days + 1)]
    nDates = len(dates)
    approx_size = nFields * nDates * nAssets * 8
    max_size = 0.1e9
    nBatches = 2 * max([np.ceil((nAssets * nFields) / maxHits), np.ceil(nFields/25), np.ceil(approx_size/max_size)])

    # Check if daily limit is likely to be reached
    if nBatches == np.ceil((nAssets * nFields) / maxHits):
        warnings.warn("Daily limit is likely to be reached, batches are delayed")
        isDelay = True
    else:
        isDelay = False

    fieldsPerBatch = int(min(np.ceil(nFields / nBatches), 8))

    # Reduce number of batches if fieldsPerBatch is too small     
    nBatches = int(np.ceil(nFields / fieldsPerBatch))

    tempSave = [None] * nBatches

    PWD = specs['path']

    # Download data in batches
    for b in range(nBatches):
        startDay = datetime.now().date()
        batchFields = fieldNames[(b*fieldsPerBatch):min((fieldsPerBatch*b)+fieldsPerBatch, nFields)]
        
        requestOptions = {"periodicitySelection": specs['periodicity'].upper()}

        # Check for overrides and fetch data
        for fieldName in batchFields:
            if fieldName in overrideDict:
                # Fetch data with overrides
                overrides = overrideDict[fieldName]
                data = get_historical_data(allTicker, [fieldName], specs['startDate'], specs['endDate'], requestOptions, overrides)
            else:
                # Fetch data without overrides
                data = get_historical_data(allTicker, [fieldName], specs['startDate'], specs['endDate'], requestOptions)

            # Save data to temp file
            tempSave[b] = os.path.join(PWD, 'Data', f'batch{b}.h5')
            save_hdf5(tempSave[b], {'d': data, 'batchFields': batchFields})

            # Delay loading of next batch if necessary
            if isDelay and datetime.now().date() <= startDay:
                print(f"{datetime.now()}: Delay loading of next batch.")
                time.sleep((datetime.now().date() + 1 - datetime.now()) * 60 * 60 * 24 + 5 * 60)

    print(f" [{datetime.now()}] Download complete.")

    # Combine data from all batches
    batchFieldsList = []
    for b in range(nBatches):
        batchData = read_hdf5(tempSave[b])
        batchFieldsList.append(batchData['d'])

    allBatches = pd.concat(batchFieldsList, ignore_index=True).reset_index(drop=True)

    # Structure the data
    result = structureFieldData(allBatches, security_name='Security', date_name='Date')

    bbgFieldData = dict()
    bbgFieldData["dates"] = result["dates"]
    bbgFieldData["instrTicker"] = result["securities"]
    bbgFieldData["fieldNames"] = result["fieldNames"]
    bbgFieldData["fieldData"] = result["fieldData"]
    return bbgFieldData



# ============================================================================
# Copyright (c) 2023, Aytekin Sari & Berner Kantonalbank. All rights reserved.
# io.py
# ============================================================================

from collections import OrderedDict
import h5py
import numpy as np
import json
import pandas as pd



# ============================================================================
# Constants
# ============================================================================

GICS_FILE = "gicsData_20221020.pkl"
BBGFIELDDATA_FILE = "bbgFieldData.pkl"

# ============================================================================
# Functions
# ============================================================================

def load_GICS(filename):
    """Load GICS data from a pickle file.

    Parameters
    ----------
    filename : str
        The name of the pickle file containing the GICS data.

    Returns
    -------
    object
        The GICS data loaded from the pickle file.
    """
    from pickle import load
    with open(filename, 'rb') as f:
        gics = load(f)
    return gics

def load_bbgFieldData(filename):
    """Load Bloomberg Field Data from a pickle file.

    Parameters
    ----------
    filename : str
        The name of the pickle file containing the Bloomberg Field Data.

    Returns
    -------
    object
        The Bloomberg Field Data loaded from the pickle file.
    """
    from pickle import load
    with open(filename, 'rb') as f:
        bbg = load(f)
    return bbg

# ============================================================================
# Helper Functions
# ============================================================================

def _OrderedDict_to_array(od):
    """Convert OrderedDict object to a standard 2d numpy array of dtype 'S'. 
    The keys are the first column of each row and the values are the rest of 
    the columns stored as a list.
    """
    return np.array([[key] + value for key, value in od.items()], dtype='S')

def _make_metadata(data):
    """Generate metadata for a given dictionary object.

    The metadata is a dictionary where each key corresponds to a key in the
    original dictionary, and the value is the class name of the object
    associated with that key. DataFrame columns are represented using '__columns__' key.
    """
    metadata = {}
    for key, value in data.items():
        if isinstance(value, dict) and not isinstance(value, OrderedDict):
            metadata[key] = _make_metadata(value)
        elif isinstance(value, pd.DataFrame):
            # Handle DataFrame columns in a generic way using a list of tuples
            column_metadata = [(col, value[col].to_numpy().__class__.__name__) for col in value.columns]
            metadata[key] = {"DataFrame": column_metadata}
        else:
            metadata[key] = value.__class__.__name__
    return metadata



# ============================================================================
# Backend Writer Functions
# ============================================================================

def _write_attrs(hdf5_file, key, value, compression):
    """Write dictionary to the HDF5 file's attributes."""
    hdf5_file.attrs[key] = json.dumps(value)

def _write_ndarray(hdf5_file, key, value, compression):
    """Write NumPy array to the HDF5 file."""
    hdf5_file.create_dataset(key, data=value, compression=compression)

def _write_OrderedDict(hdf5_file, key, value, compression):
    """Write OrderedDict object to the HDF5 file."""
    hdf5_file.create_dataset(key, data=_OrderedDict_to_array(value), compression=compression)

def _write_list(hdf5_file, key, value, compression):
    """Write list to the HDF5 file."""
    hdf5_file.create_dataset(key, data=np.asarray(value, dtype='S'), compression=compression)

def _write_scalar(hdf5_file, key, value, compression):
    """Write scalar value to the HDF5 file."""
    hdf5_file.create_dataset(key, data=np.asarray([value], dtype='S'), compression=compression)

def _write_dict(hdf5_file, key, value, compression):
    """Write a dictionary to an HDF5 file as a group."""
    group = hdf5_file.create_group(key)
    for subkey, subvalue in value.items():
        _write_to_hdf5(group, subkey, subvalue, compression)

def _write_InputParameters(hdf5_file, key, value, compression):
    """Write a dictionary to an HDF5 file as a group."""
    _write_dict(hdf5_file, key, value.to_dict(), compression)

def _write_DataFrame(hdf5_file, key, value, compression):
    """Write a pandas DataFrame to the HDF5 file."""
    group = hdf5_file.create_group(key)
    for col in value.columns:
        col_data = value[col]
        if col_data.dtype.name == 'object':  # Check if the column has an object data type
            # Convert object data type to string (or other appropriate data type)
            col_data = col_data.astype(str)
            # Handle NaN values by converting them to a unique string representation
            col_data.fillna('NaN', inplace=True)
        _write_to_hdf5(group, col, col_data.to_numpy(), compression)


def _write_to_hdf5(hdf5_file, key, value, compression):
    """Write an object to an HDF5 file using the appropriate write function."""
    writers = {
        'OrderedDict': _write_OrderedDict,
        'ndarray': _write_ndarray,
        'attrs': _write_attrs,
        'list': _write_list,
        'dict': _write_dict,
        'DataFrame': _write_DataFrame,
        'InputParameters': _write_InputParameters,
    }
    cls_name = value.__class__.__name__
    writer = writers.get(cls_name, _write_scalar)
    writer(hdf5_file, key, value, compression)

# ============================================================================
# Backend Reader Functions
# ============================================================================

def _read_attrs(value, metadata):
    """Read an attrs object from an HDF5 dataset."""
    return {k: v for k, v in value.attrs.items()}

def _read_ndarray(value, metadata):
    """Read an ndarray object from an HDF5 dataset."""
    return np.asarray(value[()])

def _read_OrderedDict(value, metadata):
    """Read an OrderedDict object from an HDF5 dataset."""
    array = np.asarray(value[()], dtype=str)
    return OrderedDict([(k, v) for k, *v in array])

def _read_list(value, metadata):
    """Read a list object from an HDF5 dataset."""
    return [s.decode() for s in value[()]]

def _read_InputParameters(value, metadata):
    """Read an InputParameters object from an HDF5 dataset."""
    return InputParameters.from_dict(_read_attrs(value, metadata))

def _read_dict(group, metadata):
    """Read a dictionary object from an HDF5 group."""
    data = {}
    for subkey in group:
        reader = _get_reader(metadata[subkey])
        data[subkey] = reader(group[subkey], metadata[subkey])
    return data

def _read_DataFrame(group, metadata):
    """Read a pandas DataFrame from an HDF5 group."""
    data = {}
    column_metadata = metadata["DataFrame"]
    #column_names = [col_name for col_name, col_dtype in column_metadata]
    for col_name, col_dtype in column_metadata:
        reader = _get_reader(col_dtype) #column_metadata[0][1])  # Use the dtype from the first element
        col_data = reader(group[col_name], col_dtype)
        if col_data.dtype.name == 'object': # Cast to string
            col_data = col_data.astype(str)
        data[col_name] = col_data
            
    return pd.DataFrame(data)


def _get_reader(value):
    """Return the appropriate reader function based on the type of value."""
    readers = {
        'OrderedDict': _read_OrderedDict,
        'ndarray': _read_ndarray,
        'attrs': _read_attrs,
        'list': _read_list,
        'dict': _read_dict,
        'DataFrame': _read_DataFrame,
        'InputParameters': _read_InputParameters,
    }
    if isinstance(value, dict) and not isinstance(value, OrderedDict):
        if 'DataFrame' in value: return readers.get('DataFrame')
        return readers.get('dict', _read_dict)
    return readers.get(value, _read_attrs)


# ============================================================================
# Save and Read public functions
# ============================================================================

def save_hdf5(filename, data, compression='gzip'):
    """
    Save a dictionary of data to an HDF5 file with optional compression.

    This function writes a dictionary where each key-value pair is saved as a dataset in an HDF5 file. 
    It supports compression for efficient storage and allows the inclusion of metadata for easy reference 
    of the internal structure of the file.

    Parameters
    ----------
    filename : str
        The name (and path) of the HDF5 file to be created for saving the data.
    data : dict
        The dictionary to be saved, where each key-value pair corresponds to a dataset.
    compression : str, optional
        The type of compression to use for storing the datasets. The default is 'gzip',
        but other compression methods supported by HDF5 can be used.

    Notes
    -----
    - The function iterates over the dictionary, writing each key-value pair as a separate dataset 
    within the HDF5 file.
    - Metadata about the data's structure is also saved for reference.
    - The function assumes the data is compatible with HDF5 storage formats.

    Examples
    --------
    >>> data = {'dataset1': array1, 'dataset2': array2}
    >>> save_hdf5('example.h5', data, compression='gzip')
    """
    # Open the HDF5 file in write mode
    with h5py.File(str(filename), 'w') as hdf5_file:

        # Iterate over the items in the dictionary
        for key, value in data.items():
            _write_to_hdf5(hdf5_file, key, value, compression)

        # Add metadata for reference on the internal structure
        metadata = _make_metadata(data)
        _write_attrs(hdf5_file, '_metadata', metadata, None)



def read_hdf5(filename):
    """
    Read data from an HDF5 file and return it as a dictionary.

    This function opens an HDF5 file and reads its contents, reconstructing a dictionary where 
    each key-value pair corresponds to a dataset within the HDF5 file. It uses metadata stored 
    in the file to properly interpret the structure and format of the data.

    Parameters
    ----------
    filename : str
        The name (and path) of the HDF5 file from which to read the data.

    Returns
    -------
    dict
        A dictionary containing the data read from the HDF5 file. Each key in the dictionary 
        corresponds to a dataset in the file, and the value is the data of that dataset.

    Notes
    -----
    - The function assumes that the HDF5 file has been written in a specific format, with 
      metadata describing the structure of the data.
    - It is important that the HDF5 file was saved using a compatible saving mechanism 
      (e.g., the `save_hdf5` function) to ensure proper reading and interpretation of the data.

    Examples
    --------
    >>> data = read_hdf5('example.h5')
    """
    # Open the HDF5 file in read mode
    with h5py.File(str(filename), 'r') as f:
        # Read the metadata
        metadata = json.loads(_read_attrs(f, None)['_metadata'])
        # Iterate over the keys
        data = {}
        for key in f:
            reader = _get_reader(metadata[key])
            data[key] = reader(f[key], metadata[key])
    return data

