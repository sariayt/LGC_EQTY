# # =============================================================================
# # IMPORTS
# # =============================================================================


# Local imports
from local_utils import *


# # =============================================================================
# # FUNCTIONS (LGCPTRUU)
# # =============================================================================


def main():

    PATH = pathlib.Path(os.path.abspath("Q:/"))
    INDEX_PATH = PATH / "ASMA" / "SYSTEME_DATEN" / "Bloomberg" / "Bloomberg SFTP" / "PM_TOOL" 

    # ------------------------------------------------------------------------
    # Chapter 1 - General Preparation
    # ------------------------------------------------------------------------

    # Get the file
    index_file = get_files(INDEX_PATH, filter_key="UT_PM_LGCPTRUU.xls")
    index_data = get_data(index_file[0], find_colname="ISIN", drop_disclaimer=True, drop_null_key="Des")

    # Drop all duplicates from the "Issuer" column and then proceed
    index_data_unique = index_data.drop_duplicates(subset='Issuer')

    # Extract the "ISIN" for unique "Issuer"
    indexISIN = index_data_unique['ISIN'].tolist()

    # Load sector and country information used, e.g., for fx conversion
    indexISIN = [f"/isin/{isin}" for isin in indexISIN]
    auxFlds = ['BOND_TO_EQY_TICKER', 'CRNCY', 'EQY_FUND_CRNCY']

    # Fetch Bloomberg data
    indexInfo = get_reference_data(indexISIN, auxFlds)

    # Filter out rows where 'BOND_TO_EQY_TICKER' is null or empty
    filtered_indexInfo = indexInfo[indexInfo['BOND_TO_EQY_TICKER'].notna() & indexInfo['BOND_TO_EQY_TICKER'].ne('')]

    # Drop rows where 'EQY_FUND_CRNCY' is null or empty
    filtered_indexInfo = filtered_indexInfo[filtered_indexInfo['EQY_FUND_CRNCY'].notna() & filtered_indexInfo['EQY_FUND_CRNCY'].ne('')]

    # Drop rows where 'CRNCY' is null or empty
    filtered_indexInfo = filtered_indexInfo[filtered_indexInfo['CRNCY'].notna() & filtered_indexInfo['CRNCY'].ne('')]    

    # Group by 'BOND_TO_EQY_TICKER' and take the first occurrence
    unique_indexInfo = filtered_indexInfo.groupby(['BOND_TO_EQY_TICKER']).first().reset_index()

    # Sort table by ticker name
    unique_indexInfo = unique_indexInfo.sort_values(by='BOND_TO_EQY_TICKER')

    # Save the cleaned and sorted data
    unique_indexInfo.to_excel('pre_indexInfo.xlsx', index=False)
    # unique_indexInfo.to_excel('P:/Project_Fixed_Income/FI_FactorStrategyEnv/factor_strategy_fi/GLB_FI/indexInfo.xlsx', index=False)

    ############################## company name ##############################

    # Group by 'BOND_TO_EQY_TICKER' and take the first occurrence
    unique_indexInfo_all = indexInfo.groupby(['BOND_TO_EQY_TICKER']).first().reset_index()
    unique_indexInfo_all= unique_indexInfo_all.sort_values(by='BOND_TO_EQY_TICKER')

    # Add "Equity" suffix to each BOND_TO_EQY_TICKER for fetching Issuer name
    equity_tickers = (unique_indexInfo_all["BOND_TO_EQY_TICKER"] + " Equity").tolist()

    # Fetch the Issuer name from Bloomberg for each Equity ticker
    # (Replace 'ISSUER_NAME' with the actual field name for Issuer if different)
    issuer_data = get_reference_data(equity_tickers, fields=['ISSUER','NAME','SECURITY_NAME','LONG_COMP_NAME','SHORT_COMPANY_NAME','ID_BB_COMPANY',
                                                             'CRNCY','EQY_FUND_CRNCY','CNTRY_OF_RISK','COUNTRY_ISO',
                                                             'CLASSIFICATION_LEVEL_1_NAME','CLASSIFICATION_LEVEL_2_NAME','CLASSIFICATION_LEVEL_3_NAME','CLASSIFICATION_LEVEL_4_NAME',
                                                             'CLASSIFICATION_LEVEL_1_CODE','CLASSIFICATION_LEVEL_2_CODE','CLASSIFICATION_LEVEL_2_CODE','CLASSIFICATION_LEVEL_4_CODE',
                                                             'ID_BB_ULTIMATE_PARENT_CO_NAME','ID_BB_ULTIMATE_PARENT_CO','ULT_PARENT_TICKER_EXCHANGE','ULT_PARENT_CNTRY_OF_RISK'
                                                             ])

    # Remove the " Equity" suffix for the final file
    issuer_data['BOND_TO_EQY_TICKER'] = issuer_data['Security'].str.replace(' Equity', '', regex=False)

    # Keep only the columns needed
    result = issuer_data[['BOND_TO_EQY_TICKER','ISSUER','NAME','SECURITY_NAME','LONG_COMP_NAME','SHORT_COMPANY_NAME','ID_BB_COMPANY',
                          'CRNCY','EQY_FUND_CRNCY','CNTRY_OF_RISK','COUNTRY_ISO',
                          'CLASSIFICATION_LEVEL_1_NAME','CLASSIFICATION_LEVEL_2_NAME','CLASSIFICATION_LEVEL_3_NAME','CLASSIFICATION_LEVEL_4_NAME',
                          'CLASSIFICATION_LEVEL_1_CODE','CLASSIFICATION_LEVEL_2_CODE','CLASSIFICATION_LEVEL_2_CODE','CLASSIFICATION_LEVEL_4_CODE',
                          'ID_BB_ULTIMATE_PARENT_CO_NAME','ID_BB_ULTIMATE_PARENT_CO','ULT_PARENT_TICKER_EXCHANGE','ULT_PARENT_CNTRY_OF_RISK'                          
                          ]]

    # Save the result as a separate Excel file
    result.to_excel('issuerInfo_eqty.xlsx', index=False)

    ############################## only members with equity price ##############################

    def get_equity_price_batch(tickers, check_date):
        try:
            # Fetch historical data for all tickers on the specified date
            historical_data = get_historical_data(
                securities=tickers,
                fields=["PX_LAST"],
                start_date=check_date,
                end_date=check_date
            )
            
            # Create a dictionary with ticker as key and price as value
            price_dict = historical_data.set_index("Security")["PX_LAST"].to_dict()
            
            # Return the dictionary with prices
            return {ticker: price_dict.get(ticker, np.nan) for ticker in tickers}
        except Exception as e:
            print(f"Error fetching historical equity prices: {e}")
            return {ticker: np.nan for ticker in tickers}  # Return NaN if error occurs

    # Define the date to check, e.g., the end date you want
    check_date = "2024-09-03"  # Use the appropriate end date here

    # Create a list of tickers with " Equity" appended
    tickers_with_equity_suffix = (unique_indexInfo["BOND_TO_EQY_TICKER"] + " Equity").tolist()

    # Get prices in one batch call
    price_dict = get_equity_price_batch(tickers_with_equity_suffix, check_date)

    # Map the prices to the `BOND_TO_EQY_TICKER` column with " Equity" suffix
    unique_indexInfo["EquityPrice"] = (unique_indexInfo["BOND_TO_EQY_TICKER"] + " Equity").map(price_dict)

    # Optional: Filter for tickers with available equity prices (non-NaN)
    equity_indexInfo = unique_indexInfo[unique_indexInfo["EquityPrice"].notna()]

    # equity_indexInfo.to_excel('indexInfo_v0.xlsx', index=False)

    # Perform a left merge, excluding columns from `result` that already exist in `equity_indexInfo`
    columns_to_merge = [col for col in result.columns if col not in equity_indexInfo.columns or col == "BOND_TO_EQY_TICKER"]

    # Merge `result` with `equity_indexInfo` on `BOND_TO_EQY_TICKER`
    equity_indexInfo = equity_indexInfo.merge(result[columns_to_merge], on="BOND_TO_EQY_TICKER", how="left")

    equity_indexInfo = equity_indexInfo[equity_indexInfo["CLASSIFICATION_LEVEL_4_CODE"].notna()]

    # Save or display the resulting DataFrame
    equity_indexInfo.to_excel('indexInfo.xlsx', index=False)
    equity_indexInfo.to_excel('C:/_sariayt/INBO/Fixed_Income/FI_Project/FI_FactorStrategy/FI_FactorStrategyEnv/factor_strategy_fi/GLB_FI/indexInfo.xlsx', index=False)
    equity_indexInfo.to_excel('C:/_sariayt/INBO/Fixed_Income/FI_Project/FI_FactorStrategy/FI_FactorStrategyEnv_sectNorm/factor_strategy_fi_sectNorm/GLB_FI/indexInfo.xlsx', index=False)
    return unique_indexInfo

if __name__ == "__main__":
    from time import time
    time_index = time()
    unique_indexInfo = main()
    print(f'[TASK completed in {int(time() - time_index)} seconds]')


