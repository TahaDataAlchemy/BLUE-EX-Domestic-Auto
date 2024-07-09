import import_ipynb
import Credentials
import pandas as pd
import numpy as np
import csv
import sys
import mysql.connector as msql
from mysql.connector import Error
from dateutil.relativedelta import relativedelta
from datetime import datetime, timedelta
import warnings
import gc
# Stopping future warnings to display
warnings.simplefilter(action='ignore', category=FutureWarning)
from Credentials import dlogin
dlogin = dlogin()
import matplotlib.pyplot as plt
import json
from datetime import datetime, timedelta
conn = msql.connect(host=dlogin.host, user=dlogin.user, password=dlogin.password, database=dlogin.database)
from logger import Logger
from CustomException import CustomException
import sys
import pymysql as msql
import pandas as pd
from datetime import datetime



def process_mom_data(json_file_path, excel_file_path, conn):
    """
    Fetching Mom Report Of Six Months
    """
    # Load input data from JSON file
    with open(json_file_path, 'r') as file:
        data = json.load(file)

    # Extract data from JSON
    mom_history = data['mom_history']
    start_date_str = mom_history['start_date_str']
    end_date_str = mom_history['end_date_str']
    n_months = mom_history['n_months']

    # Convert start_date_str to a datetime object
    c_start_date = datetime.strptime(start_date_str, "%Y-%m-%d")

    # Fetching last date of previous month
    p_start_datetime = c_start_date.replace(day=1) - timedelta(days=1)

    # Fetching previous date of current month
    p_end_datetime = c_start_date - timedelta(days=1)

    # Converting into desired format
    formatted_end_date = p_end_datetime.strftime("%Y-%m-%d")

    # Don't know why converting
    start_date = datetime.strptime(start_date_str, "%Y-%m-%d")
    end_date = datetime.strptime(end_date_str, "%Y-%m-%d")

    # Creating 6 month period
    end_date_object = datetime.strptime(end_date_str, "%Y-%m-%d")
    start_date_object = end_date_object - relativedelta(months=n_months - 1)
    p1 = start_date_object.strftime('%b-%y')
    p2 = end_date_object.strftime('%b-%y')

    # Creating an empty DataFrame
    mom = pd.DataFrame()
    mom.to_excel("MOM.xlsx")

    try:
        # Create ExcelWriter object to write to Excel file
        with pd.ExcelWriter(excel_file_path, engine='openpyxl', mode='a') as writer:
            with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 = f"SELECT * FROM blueex.mom_history"

                # Execute SQL query
                cursor.execute(q1)

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

                # Convert 'month' column to datetime format
                df['months'] = pd.to_datetime(df['month'], format='%b-%y', errors='coerce')

                # Filter DataFrame based on date range
                Mom_df = df[df['months'].between(pd.to_datetime(p1, format='%b-%y', errors='coerce'),
                                                      pd.to_datetime(p2, format='%b-%y', errors='coerce'))]

                # Write filtered DataFrame to a new sheet in the Excel file
                sheet_name = "MOM HISTORY DATA"
                Mom_df.to_excel(writer, sheet_name=sheet_name, index=False)
 
                Logger.log_info(f"MOM HISTORY Records fetched and written to Excel sheet: '{sheet_name}'")
                Logger.log_info(f"MOM HISTORY Records fetched and written to CSV: {len(Mom_df)}")
                return Mom_df

    except mysql.connector.Error as e:
        Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
        conn.rollback()  # Rollback any changes in case of error
        sys.exit()


json_file_path = 'input.json'
excel_file_path = r"C:\Users\Pc\Desktop\Domestic\MOM.xlsx"
Mom_df = process_mom_data(json_file_path, excel_file_path, conn)

def PPT3(Mom_df):
    Logger.log_info("PPT3 data processing")
    try:
        Summarize_Mom_Rep = Mom_df.groupby('month').agg(
            Active_Accounts=('acc_no', "count"),
            Normal_Shipments=("shipments", "sum"),
            Normal_Weight=('n_wght', "sum"),
            Weight_Charges=('n_wght_chgs', "sum"),
            Cash_handling=('n_oth_chgs', "sum"),
            fuel_Surcharge=('n_fsc', "sum"),
            Total_normal_rev=("n_rev", "sum")
        ).reset_index()
        
        # Calculations
        Summarize_Mom_Rep["RPS"] = round(Summarize_Mom_Rep["Total_normal_rev"] / Summarize_Mom_Rep["Normal_Shipments"], 2)
        Summarize_Mom_Rep["RPW"] = round(Summarize_Mom_Rep["Total_normal_rev"] / Summarize_Mom_Rep["Normal_Weight"], 2)
        Summarize_Mom_Rep["WPS"] = round(Summarize_Mom_Rep["Normal_Weight"] / Summarize_Mom_Rep["Normal_Shipments"], 2)
        
        # Pivot table
        Summarize_Mom_Rep = Summarize_Mom_Rep.pivot_table(index=None, columns='month', aggfunc='sum')
        
        # Reset index
        Summarize_Mom_Rep = Summarize_Mom_Rep.reset_index()
        
        # Rename columns
        Summarize_Mom_Rep.rename(columns={'index': 'AccountHeads'}, inplace=True)
        
        # Order columns
        month_columns = ['AccountHeads'] + sorted(Summarize_Mom_Rep.columns[1:], key=lambda x: pd.to_datetime(x, format='%b-%y'))
        mon_col = month_columns[1:]

        Summarize_Mom_Rep = Summarize_Mom_Rep[month_columns]
        
        for i in range(1, len(mon_col)):
            current_month = mon_col[i]
            previous_month = mon_col[i - 1]
            
            # Calculate difference between current month and previous month
            difference = Summarize_Mom_Rep[current_month] - Summarize_Mom_Rep[previous_month]
            
            # Create new column name for the difference
            new_column_name = f'{current_month} (Inc/Dec)'
            
            # Assign the difference to the new column
            Summarize_Mom_Rep[new_column_name] = difference

        for i in range(1, len(mon_col)):
            current_month = mon_col[i]
            previous_month = mon_col[i - 1]
            
            # Calculate percentage change between current month and previous month
            percentage_change = (Summarize_Mom_Rep[current_month] / Summarize_Mom_Rep[previous_month]) - 1
            
            # Create new column name for the percentage change
            new_column_name = f'{current_month} (%_chng)'
            
            # Assign the percentage change to the new column
            Summarize_Mom_Rep[new_column_name] = percentage_change

        account_heads_order = [
            'Active_Accounts', "Normal_Shipments", "Normal_Weight", "Weight_Charges", "fuel_Surcharge", "Cash_handling", "Total_normal_rev", "RPS", "RPW", "WPS"
        ]

        # Convert 'AccountHeads' column to Categorical with specified order
        Summarize_Mom_Rep['AccountHeads'] = pd.Categorical(Summarize_Mom_Rep['AccountHeads'], categories=account_heads_order, ordered=True)

        # Sort DataFrame by the custom order of 'AccountHeads'
        Summarize_Mom_Rep = Summarize_Mom_Rep.sort_values(by='AccountHeads')

        Summarize_Mom_Rep.to_excel(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT3.xlsx", index=False)
        Logger.log_info("PPT3 data processed and saved successfully.")
    except Exception as e:
        Logger.log_error(f"Exception occurred in PPT3: {e}")
        raise CustomException(e, sys)

PPT3(Mom_df)



def PPT4(Mom_df):
    Logger.log_info("PPT4 STARTED")
    try:
        dff = Mom_df['ret_ship']!=0
        for_ratios = Mom_df.groupby("month")[["shipments","n_wght"]].sum().reset_index()
        Ret_df = Mom_df[dff == True]

        Summarize_Ret_df = Ret_df.groupby("month").agg(
        active_accounts = ("acc_no","count"),
        Return_Shipment = ("ret_ship","sum"),
        Return_Weight = ('r_wght',"sum"),
        return_revenue = ('ret_revenue',"sum")
        ).reset_index()
        Summarize_Ret_df["RPS"] = round(Summarize_Ret_df["return_revenue"]/Summarize_Ret_df["Return_Shipment"],2)
        Summarize_Ret_df["RPW"] = round(Summarize_Ret_df["return_revenue"]/Summarize_Ret_df["Return_Weight"],2)
        Summarize_Ret_df["Shipment_Ret_ration"] = round(Summarize_Ret_df["Return_Shipment"]/for_ratios["shipments"]*100,2)
        Summarize_Ret_df["Weight_ret_ratio"] = round(Summarize_Ret_df["Return_Weight"]/for_ratios["n_wght"]*100,2)
        Summarize_Ret_df = Summarize_Ret_df.pivot_table(index=None, columns='month', aggfunc='sum').reset_index()
        Summarize_Ret_df.rename(columns={'index': 'AccountHeads'}, inplace=True)
        month_columns= ['AccountHeads'] + sorted(Summarize_Ret_df.columns[1:], key=lambda x: pd.to_datetime(x, format='%b-%y'))
        mon_col = month_columns[1:]
        Summarize_Ret_df = Summarize_Ret_df[month_columns]

        for i in range(1, len(mon_col)):
            current_month = mon_col[i]
            previous_month = mon_col[i - 1]
            
            # Calculate difference between current month and previous month
            difference = Summarize_Ret_df[current_month] - Summarize_Ret_df[previous_month]
            
            # Create new column name for the difference
            new_column_name = f'{current_month} (Inc/Dec)'
            
            # Assign the difference to the new column
            Summarize_Ret_df[new_column_name] = difference
        for i in range(1, len(mon_col)):
            current_month = mon_col[i]
            previous_month = mon_col[i - 1]
            
            # Calculate difference between current month and previous month
            difference = (Summarize_Ret_df[current_month]/Summarize_Ret_df[previous_month])-1
            
            # Create new column name for the difference
            new_column_name = f'{current_month} (%_chng)'
            
            # Assign the difference to the new column
            Summarize_Ret_df[new_column_name] = difference
        
        account_heads_order = [
        'active_accounts', "Return_Shipment","Shipment_Ret_ration","Return_Weight","Weight_ret_ratio","return_revenue","RPS","RPW"
        ]

        # Convert 'AccountHeads' column to Categorical with specified order
        Summarize_Ret_df['AccountHeads'] = pd.Categorical(Summarize_Ret_df['AccountHeads'], categories=account_heads_order, ordered=True)

        # Sort DataFrame by the custom order of 'AccountHeads'
        Summarize_Ret_df = Summarize_Ret_df.sort_values(by='AccountHeads')
        Summarize_Ret_df.to_excel(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT4.xlsx", index=False)
        Logger.log_info("PPT4 processed and Saved Successfully")
    except Exception as e:
        Logger.log_error(f"Exception Occur In PPT4: {e}")
        raise CustomException(e, sys)

PPT4(Mom_df)

def PPT5(json_file_path,conn):
        with open(json_file_path, 'r') as file:
            data = json.load(file)
        ppt5 = data['ppt5']
        start_date = ppt5["start_date"]
        end_date = ppt5["end_date"]

        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

        start_date_str = start_datetime.strftime('%Y-%m-%d')
        end_date_str = end_datetime.strftime('%Y-%m-%d')
        try:
            with conn.cursor() as cursor:
                        Logger.log_info("PPT5 DATA FETCHING STARTS")
                        # Check database connection
                        cursor.execute("SELECT DATABASE();")
                        record = cursor.fetchone()
                        print("Connected to Database:", record)

                        # Define SQL query to fetch records within the date range
                        q1 = "SELECT * from blueex.salesreport sr WHERE sr.normal_cn_date  BETWEEN %s AND %s "
                        
                        # Execute SQL query
                        cursor.execute(q1, (start_date, end_date))

                        # Fetch all rows
                        rows = cursor.fetchall()

                        # Create DataFrame from fetched rows
                        Ppt5 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
                        Logger.log_info("Ppt5 Data is Converted INTO dataframe")
        except mysql.connector.Error as e:
            Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
            conn.rollback()  # Rollback any changes in case of error
            sys.exit()
        
        Logger.log_info("Active This Month")
        Ppt5["normal_cn_date"] = pd.to_datetime(Ppt5["normal_cn_date"])
        Ppt5["month"] = Ppt5['normal_cn_date'].dt.strftime('%b-%Y')

        ActiveThisMonth = Ppt5[["acc_no","normal_cn_date","sp_name","month"]]
        
        def count_unique(series):
            return len(pd.Series.unique(series))

        ActiveThisMonth = pd.pivot_table(ActiveThisMonth, values='acc_no', columns='month', aggfunc=count_unique)
        Active_acc = ActiveThisMonth.reset_index()
        ActiveAcountMonthCurrent = ppt5["ActiveAcountMonthCurrent"]
        ActiveAcountMonthPrevious = ppt5["ActiveAcountMonthPrevious"]
        Active_Inc_Decs = Active_acc[ActiveAcountMonthCurrent].iloc[0] - Active_acc[ActiveAcountMonthPrevious].iloc[0]
        Active_acc["Inc_Dec"] = Active_Inc_Decs
        Active_acc = Active_acc[["index","Apr-2024","May-2024","Inc_Dec"]]

        Logger.log_info("Account Active this month but no in previous month")
        Previous = Ppt5[Ppt5['month'] == ppt5["PreviousMonth_No_ActivationPrevious"]]['acc_no']
        Current = Ppt5[Ppt5['month'] == ppt5["PreviousMonth_No_ActivationCurrent"]]['acc_no']
        Previous_1 = Ppt5[Ppt5['month'] == ppt5["PreviousMonth_No_ActivationPrevious-1"]]['acc_no']
        accounts_in_Current_not_in_Prev = Current[~Current.isin(Previous)]
        Active_this_Month_not_in_Previous_Months = accounts_in_Current_not_in_Prev.nunique()
        accounts_in_Prev_not_in_Prev_1 = Previous[~Previous.isin(Previous_1)]
        Active_Previous_Month_not_in_Prev1_Months = accounts_in_Prev_not_in_Prev_1.nunique()
        Active_Inc_Dec = Active_this_Month_not_in_Previous_Months - Active_Previous_Month_not_in_Prev1_Months
        Row2 = pd.DataFrame({
            "index": ["Active this Month but not in Previous Months"], 
            ppt5["PreviousMonth_No_ActivationPrevious"]: [Active_Previous_Month_not_in_Prev1_Months], 
            ppt5["PreviousMonth_No_ActivationCurrent"]: [Active_this_Month_not_in_Previous_Months], 
            "Inc_Dec": [Active_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row2], axis=0, ignore_index=True)

        Logger.log_info("New Active Accounts")

        start_date = ppt5["start_date"]
        end_date = ppt5["end_date"]
        start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
        end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

        start_date_str = start_datetime.strftime('%Y-%m-%d')
        end_date_str = end_datetime.strftime('%Y-%m-%d')
        try:
            with conn.cursor() as cursor:
                        # Check database connection
                        cursor.execute("SELECT DATABASE();")
                        record = cursor.fetchone()
                        print("Connected to Database:", record)

                        # Define SQL query to fetch records within the date range
                        q1 = "SELECT * from blueex.salesreport sr LEFT JOIN acc_form af ON sr.acc_no = af.acc_no   WHERE sr.normal_cn_date  BETWEEN %s AND %s "
                        
                        # Execute SQL query
                        cursor.execute(q1, (start_date, end_date))

                        # Fetch all rows
                        rows = cursor.fetchall()

                        # Create DataFrame from fetched rows
                        NewActive = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
                        Logger.log_info("Acc form Fetches")
                        Logger.log_info("NewActive DataFrame Created")
        except mysql.connector.Error as e:
                        Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
                        conn.rollback()  # Rollback any changes in case of error
                        sys.exit()

        NewActive["normal_cn_date"] = pd.to_datetime(NewActive["normal_cn_date"])
        NewActive["month"] = NewActive['normal_cn_date'].dt.strftime('%b-%Y')
        duplicate_columns = NewActive.columns[NewActive.columns.duplicated()]
        NewActive = NewActive.loc[:, ~NewActive.columns.duplicated()]
        NewActive['first_cn'] = pd.to_datetime(NewActive['first_cn'])
        shipments_2024 = NewActive[NewActive['first_cn'].dt.year == 2024]
        shipments_2024['first_shipment_month'] = shipments_2024['first_cn'].dt.strftime('%b-%Y')
        monthly_New_Acc = shipments_2024.groupby('first_shipment_month')['acc_no'].nunique().reset_index()
        monthly_New_Acc = monthly_New_Acc.pivot_table(columns='first_shipment_month', values='acc_no', aggfunc='sum')
        prev = ppt5["ActiveAcountMonthPrevious"]
        curr = ppt5["ActiveAcountMonthCurrent"]
        monthly_New_Acc = monthly_New_Acc[[prev,curr]]
        New_acc_Inc_Dec = monthly_New_Acc[curr].values[0] - monthly_New_Acc[prev].values[0]
        Row3 = pd.DataFrame({
            "index": ["New Active"], 
            ppt5["ActiveAcountMonthPrevious"]: [monthly_New_Acc[prev].values[0]], 
            ppt5["ActiveAcountMonthCurrent"]: [monthly_New_Acc[curr].values[0]], 
            "Inc_Dec": [New_acc_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row3], axis=0, ignore_index=True)

        Logger.log_info("Revived Row")

        Revived_Previous = Active_Previous_Month_not_in_Prev1_Months - monthly_New_Acc[prev].values[0]
        Revived_Curr = Active_this_Month_not_in_Previous_Months - monthly_New_Acc[curr].values[0]
        Revived_Inc_Dec = Revived_Curr - Revived_Previous
        Row4 = pd.DataFrame({
            "index": ["Revived"], 
            ppt5["ActiveAcountMonthPrevious"]: [Revived_Previous], 
            ppt5["ActiveAcountMonthCurrent"]: [Revived_Curr], 
            "Inc_Dec": [Revived_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row4], axis=0, ignore_index=True)

        Logger.log_info("Retained From Previous")
        Retained_Prev  = Active_acc[prev].iloc[0] - Active_Previous_Month_not_in_Prev1_Months
        Retained_Curr = Active_acc[curr].iloc[0] - Active_this_Month_not_in_Previous_Months
        Retained_Inc_Dec = Retained_Curr - Retained_Prev
        Row5 = pd.DataFrame({
            "index": ["Retained from Previous Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [Retained_Prev], 
            ppt5["ActiveAcountMonthCurrent"]: [Retained_Curr], 
            "Inc_Dec": [Retained_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row5], axis=0, ignore_index=True)

        Logger.log_info("Active in Previous Month but not this month")

        Previous_1_Month = NewActive[NewActive["month"] == ppt5["PreviousMonth_No_ActivationPrevious-1"]]["acc_no"].unique()
        PreviousMonth = NewActive[NewActive["month"] == ppt5["PreviousMonth_No_ActivationPrevious"]]["acc_no"].unique()
        Curr_Month = NewActive[NewActive["month"] == ppt5["PreviousMonth_No_ActivationCurrent"]]["acc_no"].unique()

        PreviousMonthNotInCurrent = np.setdiff1d(PreviousMonth, Curr_Month)
        Acc_PreviousMonthNotInCurrent = len(PreviousMonthNotInCurrent)

        Previous_1_not_IN_Previous = np.setdiff1d(Previous_1_Month, PreviousMonth)
        Acc_Previous_1_not_IN_Previous = len(Previous_1_not_IN_Previous)

        Acc_Inc_Dec = Acc_PreviousMonthNotInCurrent - Acc_Previous_1_not_IN_Previous
        Row6 = pd.DataFrame({
            "index": ["Active in Previous Month but not this month"], 
            ppt5["ActiveAcountMonthPrevious"]: [Acc_Previous_1_not_IN_Previous], 
            ppt5["ActiveAcountMonthCurrent"]: [Acc_PreviousMonthNotInCurrent], 
            "Inc_Dec": [Acc_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row6], axis=0, ignore_index=True)
        Logger.log_info("Retained From Previous Completed")

        Logger.log_info("CRF Created this Month Starts")

        ppt5_crf = data['ppt5']["month_ranges"]
        month_ranges = ppt5_crf
        results = []
        try:
            with conn.cursor() as cursor:
                # Loop through each month range
                for start_date, end_date in month_ranges:
                    # Define SQL query to count distinct acc_no within the date range
                    sql_query = """
                        SELECT COUNT(DISTINCT acc_no) AS distinct_count
                        FROM blueex.acc_form
                        WHERE creation_date BETWEEN %s AND %s
                    """
                    
                    # Execute the SQL query with date range parameters
                    cursor.execute(sql_query, (start_date, end_date))
                    
                    # Fetch the result (distinct count of acc_no)
                    distinct_count = cursor.fetchone()[0]  # Get the first column of the first row
                    
                    # Format the month as 'Month-YYYY' (e.g., 'Jan-2024')
                    month_year = datetime.strptime(start_date, '%Y-%m-%d').strftime('%b-%Y')
                    
                    # Append result to the list as a dictionary
                    results.append({'Month': month_year, 'Distinct_count': distinct_count})

        except mysql.connector.Error as e:
                    Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
                    conn.rollback()  # Rollback any changes in case of error
                    sys.exit()

        # Create a pandas DataFrame from the results list
        Crf_Created_This_month = pd.DataFrame(results)

        Inc_Dec_Crf = Crf_Created_This_month.iloc[2][1] - Crf_Created_This_month.iloc[1][1]
        Row7 = pd.DataFrame({
            "index": ["CRF Created this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [Crf_Created_This_month.iloc[1][1]], 
            ppt5["ActiveAcountMonthCurrent"]: [Crf_Created_This_month.iloc[2][1]], 
            "Inc_Dec": [Inc_Dec_Crf]
        })
        Active_acc = pd.concat([Active_acc, Row7], axis=0, ignore_index=True)
        
        Logger.log_info("CRF Approved this Month")

        results = []

# Establish connection to the database
        try:
            with conn.cursor() as cursor:
                # Loop through each month range
                for start_date, end_date in month_ranges:
                    # Define SQL query to count distinct acc_no within the date range
                    sql_query = """
                        SELECT COUNT(DISTINCT acc_no) 
                        FROM blueex.acc_form
                        WHERE finance_approval BETWEEN %s AND %s
                        AND second_approval BETWEEN %s AND %s;
                    """
                    
                    # Execute the SQL query with date range parameters
                    cursor.execute(sql_query, (start_date, end_date, start_date, end_date))
                    
                    # Fetch the result (distinct count of acc_no)
                    distinct_count = cursor.fetchone()[0]  # Get the first column of the first row
                    
                    # Format the month as 'Month-YYYY' (e.g., 'Jan-2024')
                    month_year = pd.to_datetime(start_date).strftime('%b-%Y')
                    
                    # Append result to the list as a dictionary
                    results.append({'Month': month_year, 'Distinct_count': distinct_count})


                    # Create a pandas DataFrame from the results list
                    crf_App = pd.DataFrame(results)

        except mysql.connector.Error as e:
                    Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
                    conn.rollback()  # Rollback any changes in case of error
                    sys.exit()
        
        crf_App_Inc_Dec = crf_App.iloc[2][1] - crf_App.iloc[1][1]
        Row8 = pd.DataFrame({
            "index": ["CRF Approved this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [crf_App.iloc[1][1]], 
            ppt5["ActiveAcountMonthCurrent"]: [crf_App.iloc[2][1]], 
            "Inc_Dec": [crf_App_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row8], axis=0, ignore_index=True)
        Logger.log_info("CRF Approved this Month Complete")

        Logger.log_info("CRF APPROVE AND CREATED THIS MONTH Starts")
        results = []
        try:
            with conn.cursor() as cursor:
                for start_date, end_date in month_ranges:
                    # Define SQL query to count distinct acc_no within the specified month range
                    sql_query = """
                        SELECT COUNT(DISTINCT acc_no)
                        FROM blueex.acc_form
                        WHERE Creation_date BETWEEN %s AND %s
                        AND finance_approval BETWEEN %s AND %s
                        AND second_approval BETWEEN %s AND %s;
                    """
                    
                    # Execute the SQL query with month range parameters
                    cursor.execute(sql_query, (start_date, end_date, start_date, end_date, start_date, end_date))
                    
                    # Fetch the result (distinct count of acc_no)
                    distinct_count = cursor.fetchone()[0]  # Get the first column of the first row
                    
                    # Format the month as 'Month-YYYY' (e.g., 'Jan-2024')
                    month_year = pd.to_datetime(start_date).strftime('%b-%Y')
                    
                    # Append result to the list as a dictionary
                    results.append({'Month': month_year, 'Distinct_count': distinct_count})

        except mysql.connector.Error as e:
                    Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
                    conn.rollback()  # Rollback any changes in case of error
                    sys.exit()
        # Create a pandas DataFrame from the results list
        CRF_APP_CRE = pd.DataFrame(results)
        crf_CRF_APP_CRE = CRF_APP_CRE.iloc[2][1] - CRF_APP_CRE.iloc[1][1]
        Row9 = pd.DataFrame({
            "index": ["CRF Created & Approved this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [CRF_APP_CRE.iloc[1][1]], 
            ppt5["ActiveAcountMonthCurrent"]: [CRF_APP_CRE.iloc[2][1]], 
            "Inc_Dec": [crf_CRF_APP_CRE]
        })
        Active_acc = pd.concat([Active_acc, Row9], axis=0, ignore_index=True)
        Logger.log_info("CRF Created & Approved this Month Ends")

        Logger.log_info("CRF Not Created but approved this Month Starts")
        crf_App["Crf_Not_created_Aprroved"] = crf_App["Distinct_count"] - CRF_APP_CRE["Distinct_count"] 
        Crf_NotCreated_app_Inc_Dec = crf_App.iloc[2][2] - crf_App.iloc[1][2]
        Row10 = pd.DataFrame({
            "index": ["CRF Not Created but approved this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [crf_App.iloc[1]['Crf_Not_created_Aprroved']], 
            ppt5["ActiveAcountMonthCurrent"]: [crf_App.iloc[2]['Crf_Not_created_Aprroved']], 
            "Inc_Dec": [Crf_NotCreated_app_Inc_Dec]
        })
        Active_acc = pd.concat([Active_acc, Row10], axis=0, ignore_index=True)
        Logger.log_info("CRF Not Created but approved this Month Ends")

        Logger.log_info("CRF Created, Approved and Active this Month")

        try:
            with conn.cursor() as cursor:
                results = []  # To store the results for each month range
                
                # Define the SQL query template
                sql_query = """
                    SELECT COUNT(DISTINCT acc_no) AS Unique_acc_count
                    FROM blueex.acc_form
                    WHERE 
                        YEAR(creation_date) = %s AND MONTH(creation_date) = %s
                        AND YEAR(finance_approval) = %s AND MONTH(finance_approval) = %s
                        AND YEAR(second_approval) = %s AND MONTH(second_approval) = %s
                        AND YEAR(first_cn) = %s AND MONTH(first_cn) = %s;
                """
                
                # Loop through each month range
                for start_date, end_date in month_ranges:
                    # Extract year and month from start_date
                    year = int(start_date[:4])
                    month = int(start_date[5:7])
                    
                    # Execute the SQL query with parameters for the current month range
                    cursor.execute(sql_query, (year, month, year, month, year, month, year, month))
                    
                    # Fetch the result
                    result = cursor.fetchone()
                    month_year = pd.to_datetime(start_date).strftime('%b-%Y')

                    
                    # Append the result to the list
                    results.append({'Month': f'{month_year}', 'Unique_acc_count': result[0]})
                
                # Create a pandas DataFrame from the results list
                crf_created_app_first_cn = pd.DataFrame(results)
            
        except mysql.connector.Error as e:
                    Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
                    conn.rollback()  # Rollback any changes in case of error
                    sys.exit()
        crf_created_app_first_cn_INC_DEC = crf_created_app_first_cn.iloc[2]["Unique_acc_count"] - crf_created_app_first_cn.iloc[1]["Unique_acc_count"] 
        Row11 = pd.DataFrame({
            "index": ["CRF Created, Approved and Active this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [crf_created_app_first_cn.iloc[1]["Unique_acc_count"]], 
            ppt5["ActiveAcountMonthCurrent"]: [ crf_created_app_first_cn.iloc[2]["Unique_acc_count"]], 
            "Inc_Dec": [crf_created_app_first_cn_INC_DEC]
        })
        Active_acc = pd.concat([Active_acc, Row11], axis=0, ignore_index=True)
        Logger.log_info("CRF Created, Approved and Active this Month Ends")


        Logger.log_info("CRF Not Created & Approved this Month but Active this Month Starts")
        # Initialize a list to store results
        results = []

        # Loop through each month range
        for start_date, end_date in month_ranges:
            # Convert start_date and end_date to datetime objects
            start_date = pd.to_datetime(start_date)
            end_date = pd.to_datetime(end_date)

            # Filter the DataFrame based on the specified conditions
            filtered_df = NewActive[
                (pd.to_datetime(NewActive['first_cn'], format='%b-%Y') >= start_date) &  # first_cn within the current month range
                (pd.to_datetime(NewActive['first_cn'], format='%b-%Y') <= end_date) &
                (pd.to_datetime(NewActive['creation_date']) < start_date) &  # creation_date before the current month
                (pd.to_datetime(NewActive['finance_approval']) < start_date) &  # finance_approval before the current month
                (pd.to_datetime(NewActive['second_approval']) < start_date)  # second_approval before the current month
            ]

            # Count the number of distinct acc_no in the filtered DataFrame
            unique_acc_count = filtered_df['acc_no'].nunique()

            # Append the result to the results list
            results.append({
                'Month': f"{start_date.strftime('%b')} {start_date.year}",
                'Unique_acc_count': unique_acc_count
            })

        # Create a DataFrame from the results list
        Crf_Not_Created_But_Active = pd.DataFrame(results)
        Crf_Not_Created_But_Active_Inc_DEC = Crf_Not_Created_But_Active.iloc[2][1] - Crf_Not_Created_But_Active.iloc[1][1]
        Row12 = pd.DataFrame({
            "index": ["CRF Not Created & Approved this Month but Active this Month"], 
            ppt5["ActiveAcountMonthPrevious"]: [Crf_Not_Created_But_Active.iloc[1][1]], 
            ppt5["ActiveAcountMonthCurrent"]: [Crf_Not_Created_But_Active.iloc[2][1]], 
            "Inc_Dec": [Crf_Not_Created_But_Active_Inc_DEC]
        })
        Active_acc = pd.concat([Active_acc, Row12], axis=0, ignore_index=True)
        Logger.log_info("CRF Not Created & Approved this Month but Active this Month Ends")
        Active_acc.to_excel(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT5.xlsx", index=False)
        Logger.log_info("PPT5 processed and Saved Successfully")
json_file_path = 'input.json'
PPT5(json_file_path,conn)    



def PPT7(json_file_path, conn):
    Logger.log_info("PPT7 starts")
    
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    
    ppt7 = data['ppt7']
    start_date = ppt7["start_date"]
    end_date = ppt7["end_date"]
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')
    
    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    
    try:
        with conn.cursor() as cursor:
            # Check database connection
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print("Connected to Database:", record)
            
            # Define SQL query to fetch records within the date range
            q1 = """
                SELECT sr.*
                FROM blueex.salesreport sr 
                WHERE sr.normal_cn_date BETWEEN %s AND %s
            """
            
            # Execute SQL query
            cursor.execute(q1, (start_date, end_date))
            
            # Fetch all rows
            rows = cursor.fetchall()
            
            # Create DataFrame from fetched rows
            PPT7 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    
    except msql.Error as e:
        Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
        conn.rollback()  # Rollback any changes in case of error
        sys.exit()
    
    PPT7['normal_cn_date'] = pd.to_datetime(PPT7['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    PPT7["Month_year"] = PPT7['normal_cn_date'].dt.strftime('%b-%Y')

    PPT7_Group = PPT7.groupby(["Month_year","master_dest","dest_zone"]).agg(
        Shipments = ("normal_cn","count")

    ).reset_index()

    Total_PPT7_Group = PPT7.groupby(["Month_year", "master_dest"]).agg(
        Shipments=("normal_cn", "count")
    ).reset_index()
    PPT7_Group = pd.pivot_table(PPT7_Group,index=["master_dest","Month_year"],columns="dest_zone",values="Shipments",aggfunc="sum").reset_index()

    PPT7_Group = pd.merge(Total_PPT7_Group,PPT7_Group,on = ["master_dest","Month_year"],how = "outer")

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['Shipments'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='Shipments')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(PPT7_Group)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C","Shipments"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('Shipments', ppt7["Curr_Month"]), ascending=False)

    PPT7_Group = round(sorted_top_values,2)

    PPT7_Group.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT7.csv", index=False)    
    Logger.log_info("PPT7 processed and Saved Successfully")


# Define the JSON file path
json_file_path = 'input.json'

# Call the PPT7 function
PPT7(json_file_path, conn)

def PPT8(Mom_df,json_file_path,conn):
     Logger.log_info("PPT8 starts")
     PPT_8 = Mom_df.groupby("month")['shipments'].sum().reset_index()
     PPT_8.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT8.csv", index=False)    
     Logger.log_info("PPT 8 COMPLETED AND SAVED")
     return PPT_8

json_file_path = 'input.json'
PPT_8 = PPT8(Mom_df,json_file_path,conn)

def PPT9(Mom_df,json_file_path,conn):
    Logger.log_info("PPT9 STARTS")
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt9 = data['ppt9']
    Summarize_PPT9 = Mom_df.groupby("month").agg(
    Return_Shipments = ('ret_ship',"sum"),
    WC = ("r_s_wc","sum"),
    Zone_A = ('r_s_za',"sum"),
    Zone_B = ('r_s_zb',"sum"),
    Zone_C = ('r_s_zc',"sum"),
    With_charges  = ('r_s_chrg',"sum"),
    With_no_charges = ('r_s_nchrg',"sum")
    ).reset_index()
    numeric_columns = ['Return_Shipments', 'WC', 'Zone_A', 'Zone_B', 'Zone_C', 'With_charges', 'With_no_charges']
    Summarize_PPT9[numeric_columns] = Summarize_PPT9[numeric_columns].apply(pd.to_numeric)

    # Pivot the DataFrame to reshape it
    Summarize_PPT9 = pd.pivot_table(Summarize_PPT9, columns='month', aggfunc='sum')
    Summarize_PPT9 = Summarize_PPT9.reset_index()
    Summarize_PPT9.rename(columns={"index":"Description"},inplace=True)
    columns_to_sort = [col for col in Summarize_PPT9.columns if col != 'Description']
    sorted_columns = pd.to_datetime(columns_to_sort, format='%b-%y').sort_values()
    Summarize_PPT9 = Summarize_PPT9[['Description'] + sorted_columns.strftime('%b-%y').tolist()]
    num_columns = len(Summarize_PPT9.columns)
    last_three_columns = Summarize_PPT9.iloc[:, num_columns - 3:]
    description_column = Summarize_PPT9["Description"].reset_index(drop=True)
    Summarize_PPT9 = pd.concat([description_column, last_three_columns], axis=1)
    Total_ship = PPT_8[PPT_8["month"] == ppt9["Curr_Month"]]["shipments"].values[0]
    Summarize_PPT9[f"{ppt9['Curr_Month']}_%"]= round((Summarize_PPT9[ppt9["Curr_Month"]]/Total_ship)*100,2)
    Summarize_PPT9[f"{ppt9['Prev_Month']}_%"] = round((Summarize_PPT9[ppt9["Prev_Month"]]/Total_ship)*100,2)
    Summarize_PPT9[f"{ppt9['Curr_Month']}_%"] = round((Summarize_PPT9[ppt9["Curr_Month"]]/Total_ship)*100,2)
    Summarize_PPT9["INC_DEC"] = Summarize_PPT9[ppt9["Curr_Month"]] - Summarize_PPT9[ppt9["Prev_Month"]]
    Summarize_PPT9.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT9_first_table.csv", index=False)    
    Logger.log_info("PPT9_1 processed and Saved Successfully")

    Logger.log_info("PPT_9 SECOND TABLE")

    start_date = ppt9["start_date"]  # Change this to the desired start date
    end_date = ppt9["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    try:
        with conn.cursor() as cursor:
                    # Check database connection
                    cursor.execute("SELECT DATABASE();")
                    record = cursor.fetchone()
                    print("Connected to Database:", record)

                    # Define SQL query to fetch records within the date range
                    q1 =  """SELECT sr.master_origin , sr.master_dest , rr.*
                            FROM blueex.salesreport sr 
                            JOIN blueex.returnreport rr
                            ON sr.normal_cn = rr.normal_cn
                            WHERE sr.normal_cn_date BETWEEN %s AND %s
                        
                        """
                    
                    # Execute SQL query
                    cursor.execute(q1, (start_date, end_date))

                    # Fetch all rows
                    rows = cursor.fetchall()

                    # Create DataFrame from fetched rows
                    PPT9_1 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    except msql.Error as e:
        Logger.log_error(f"Error occurred while connecting to MySQL: {e}")
        conn.rollback()  # Rollback any changes in case of error
        sys.exit()
    
    PPT9_1['normal_cn_date'] = pd.to_datetime(PPT9_1['normal_cn_date'])
    # Convert datetime to string with specified format '%b-%Y'
    PPT9_1["Month_year"] = PPT9_1['normal_cn_date'].dt.strftime('%b-%Y')
    PPT9_1_GROUP = (
    PPT9_1.groupby(["Month_year", "master_dest"])["return_cn"]
    .count()
        .reset_index(name="count")  # Resetting index and naming the count column
    )

    # Creating an empty DataFrame to store the top 10 values per month
    top_values_by_month = pd.DataFrame()

    # Iterating over each month in the grouped DataFrame
    for month in PPT9_1_GROUP["Month_year"].unique():
        # Filtering data for the current month
        month_data = PPT9_1_GROUP[PPT9_1_GROUP["Month_year"] == month]
        
        # Finding the top 10 values for this month based on count
        top_values = month_data.nlargest(10, "count")
        
        # Appending the top values for this month to the result DataFrame
        top_values_by_month = pd.concat([top_values_by_month, top_values])

    # Resetting the index of the result DataFrame
    top_values_by_month = top_values_by_month.reset_index(drop=True)
    Slide_9  = pd.pivot_table(top_values_by_month,index="master_dest",columns="Month_year",values="count").reset_index().fillna(0)
    Slide_9[f"{ppt9['Curr_Month']}_%"] = round((Slide_9[ppt9["Curr_Month_1"]]/Summarize_PPT9.iloc[0][ppt9["Curr_Month"]])*100,2)
    Slide_9[f"{ppt9['Prev_Month']}_%"] = round((Slide_9[ppt9["Prev_Month_1"]]/Summarize_PPT9.iloc[0][ppt9["Prev_Month"]])*100,2)
    Slide_9["INC_DEC"] = Slide_9[ppt9["Curr_Month_1"]] - Slide_9[ppt9["Prev_Month_1"]]
    # Slide_9.sort_values(by=ppt9["Curr_Month"],ascending=False)
    Slide_9.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT9_Second_table.csv", index=False)    
    Logger.log_info("PPT9_2 processed and Saved Successfully")



json_file_path = 'input.json'
PPT9(Mom_df,json_file_path,conn)

def PPT10(Mom_df,json_file_path,conn):
    Logger.log_info("PPT10 STARTS")
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt10 = data['ppt10']     
    month_names = ppt10["Month_list"]
    working_days = ppt10["working_days"]

    # Create working_days_df DataFrame
    working_days_df = pd.DataFrame({
        'month': month_names,
        'working_days': working_days
    })
    Summarize_PPT10 = Mom_df.groupby('month').agg(
    Normal_Weight=('n_wght', 'sum'),
    WC=('n_wght_wc', 'sum'),
    Zone_A=('n_wght_za', 'sum'),
    Zone_B=('n_wght_zb', 'sum'),
    Zone_C=('n_wght_zc', 'sum')
    ).reset_index()
    for index, row in working_days_df.iterrows():
        month = row['month']
        working_days = row['working_days']
        
        # Filter Summarize_PPT10 for the current month
        data = Summarize_PPT10[Summarize_PPT10['month'] == month]
        
        if len(data) > 0:
            # Calculate moving average
            mov_avg = round(data['Normal_Weight'] / working_days, 2)
            
            # Assign the calculated moving average to 'Mov_Avg' column in Summarize_PPT10
            Summarize_PPT10.loc[Summarize_PPT10['month'] == month, 'Mov_Avg'] = mov_avg.values[0]  # Access the first value in mov_avg list
            Summarize_PPT10 = Summarize_PPT10.fillna(0)
    Summarize_PPT10 = pd.pivot_table(Summarize_PPT10,columns="month",aggfunc="sum").reset_index()
    Summarize_PPT10.rename(columns={"index": "Description"}, inplace=True)
    columns_to_sort = [col for col in Summarize_PPT10.columns if col != 'Description']
    sorted_columns = sorted(columns_to_sort, key=lambda x: pd.to_datetime(x, format='%b-%y'))

    # Reorder columns in Summarize_PPT9 based on sorted column names
    Summarize_PPT10 = Summarize_PPT10[['Description'] + sorted_columns]

    num_columns = len(Summarize_PPT10.columns)

    # Use iloc to select the last three columns
    last_three_columns = Summarize_PPT10.iloc[:, num_columns - 2:]

    # Reset the index of the "Description" column to match the last_three_columns
    description_column = Summarize_PPT10["Description"].reset_index(drop=True)

    # Concatenate the "Description" column with the last_three_columns horizontally
    Summarize_PPT10 = pd.concat([description_column, last_three_columns], axis=1)

    TotalWeigh_2 = Summarize_PPT10.iloc[:,-1][1]
    TotalWeigh_1 = Summarize_PPT10.iloc[:,1][1]

    Summarize_PPT10[f"{ppt10['Curr_Month']}_%"] =(Summarize_PPT10.iloc[:,-1]/TotalWeigh_2)*100
    Summarize_PPT10[f"{ppt10['Prev_Month']}_%"] =(Summarize_PPT10.iloc[:,1]/TotalWeigh_1)*100

    Summarize_PPT10 = round(Summarize_PPT10,2)
    Summarize_PPT10["INC_DEC"] = round(Summarize_PPT10.iloc[:,2] - Summarize_PPT10.iloc[:,1])
    Summarize_PPT10.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT10_1_table.csv", index=False)    
    Logger.log_info("PPT10_1 processed and Saved Successfully")

    Logger.log_info("PPT10_2 starts")

    start_date = ppt10["start_date"] # Change this to the desired start date
    end_date = ppt10["end_date"]
    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.master_origin , sr.normal_cn_date ,sr.master_dest,sr.dest_zone,sr.weight,sr.weight_band
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                PPT10_2 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    total_Weight = PPT10_2["weight"].sum()
    PPT10_2['normal_cn_date'] = pd.to_datetime(PPT10_2['normal_cn_date'])
    # Convert datetime to string with specified format '%b-%Y'
    PPT10_2["Month_year"] = PPT10_2['normal_cn_date'].dt.strftime('%b-%Y')
    PPT10_1_GROUP = (
        PPT10_2.groupby(["Month_year", "master_dest","dest_zone"])["weight"]
        .sum()
        .reset_index(name="W_Total")  # Resetting index and naming the count column
    )
    PPT10_2['normal_cn_date'] = pd.to_datetime(PPT10_2['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    PPT10_2["Month_year"] = PPT10_2['normal_cn_date'].dt.strftime('%b-%Y')
    PPT10_1_GROUP = (
        PPT10_2.groupby(["Month_year", "master_dest","dest_zone"])["weight"]
        .sum()
        .reset_index(name="W_Total")  # Resetting index and naming the count column
    )
    PPT10_1_GROUP = (
    PPT10_2.groupby(["Month_year", "master_dest","dest_zone"])["weight"]
    .sum()
    .reset_index(name="W_Total")  # Resetting index and naming the count column
    )
    PPT10_1_GROUP = pd.pivot_table(PPT10_1_GROUP,index=["master_dest","Month_year"],columns="dest_zone",values="W_Total",aggfunc="sum").reset_index()
    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['A'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='A')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(PPT10_1_GROUP)
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C"]).fillna(0).reset_index()
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('A', ppt10['Curr_Month_year_f']), ascending=False)
    sorted_top_values.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT10_2_table.csv", index=False)    
    Logger.log_info("PPT10_2 processed and Saved Successfully")

json_file_path = 'input.json'
PPT10(Mom_df,json_file_path,conn)

Logger.log_info("PPT11 Starts")

def PPT11(Mom_df):
    SLIDE_11 = Mom_df.groupby("month")["n_wght"].sum().reset_index()
    SLIDE_11.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT11_table.csv", index=False)    
    Logger.log_info("PPT11 processed and Saved Successfully")
PPT11(Mom_df)

def PPT12(Mom_df,json_file_path,conn):
    Logger.log_info("PPT12 Starts")

    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt12 = data['Ppt12']     
    Summarize_PPT12 = Mom_df.groupby("month").agg(
        Return_Weight = ("r_wght","sum"),
        WC = ("r_wght_wc","sum"),
        Zone_A = ("r_wght_za","sum"),
        Zone_B = ("r_wght_zb","sum"),
        Zone_C = ('r_wght_zc',"sum"),
        Wght_chrgs = ('r_wght_chrg','sum'),
        Wght_no_chrgs = ('r_wght_nchrg',"sum")
    ).reset_index()

    Summarize_PPT12 = pd.pivot_table(Summarize_PPT12,columns="month",aggfunc="sum").reset_index()
    Summarize_PPT12.rename(columns={"index": "Description"}, inplace=True)
    columns_to_sort = [col for col in Summarize_PPT12.columns if col != 'Description']
    sorted_columns = sorted(columns_to_sort, key=lambda x: pd.to_datetime(x, format='%b-%y'))

    # Reorder columns in Summarize_PPT9 based on sorted column names
    Summarize_PPT12 = Summarize_PPT12[['Description'] + sorted_columns]

    num_columns = len(Summarize_PPT12.columns)

    # Use iloc to select the last three columns
    last_three_columns = Summarize_PPT12.iloc[:, num_columns - 2:]

    # Reset the index of the "Description" column to match the last_three_columns
    description_column = Summarize_PPT12["Description"].reset_index(drop=True)

    # Concatenate the "Description" column with the last_three_columns horizontally
    Summarize_PPT12 = pd.concat([description_column, last_three_columns], axis=1)

    TotalWeigh_2 = Summarize_PPT12.iloc[:,-1][1]
    TotalWeigh_1 = Summarize_PPT12.iloc[:,1][1]

    Summarize_PPT12[f"{ppt12['Curr_Month']}_%"] =(Summarize_PPT12.iloc[:,-1]/TotalWeigh_2)*100
    Summarize_PPT12[f"{ppt12['Prev_Month']}_%"] =(Summarize_PPT12.iloc[:,1]/TotalWeigh_1)*100

    Summarize_PPT12 = round(Summarize_PPT12,2)
    Summarize_PPT12["INC_DEC"] = round(Summarize_PPT12.iloc[:,2] - Summarize_PPT12.iloc[:,1])
    Summarize_PPT12.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT12_table.csv", index=False)    
    Logger.log_info("PPT12 processed and Saved Successfully")
    
    Logger.log_info("PPT12_1 Starts")

    start_date = ppt12["start_date"] # Change this to the desired start date
    end_date = ppt12["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.master_origin ,sr.master_dest,sr.dest_zone,sr.weight,sr.weight_band,rr.*
                        FROM blueex.salesreport sr 
                        JOIN blueex.returnreport rr
                        ON sr.normal_cn = rr.normal_cn
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                ppt12_1 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    ppt12_1['normal_cn_date'] = pd.to_datetime(ppt12_1['normal_cn_date'])

    ppt12_1["Month_year"] = ppt12_1['normal_cn_date'].dt.strftime('%b-%Y')

    PPT12_1_GROUP = (
    ppt12_1.groupby(["Month_year", "master_dest"])["weight"]
    .sum()
    .reset_index(name="SumOfWght")  # Resetting index and naming the count column
    )

    # Creating an empty DataFrame to store the top 10 values per month
    top_values_by_month = pd.DataFrame()

    # Iterating over each month in the grouped DataFrame
    for month in PPT12_1_GROUP["Month_year"].unique():
        # Filtering data for the current month
        month_data =PPT12_1_GROUP[PPT12_1_GROUP["Month_year"] == month]
        
        # Finding the top 10 values for this month based on count
        top_values = month_data.nlargest(10, "SumOfWght")
        
        # Appending the top values for this month to the result DataFrame
        top_values_by_month = pd.concat([top_values_by_month, top_values])

    # Resetting the index of the result DataFrame
    top_values_by_month = top_values_by_month.reset_index(drop=True)

    Slide_12  = pd.pivot_table(top_values_by_month,index="master_dest",columns="Month_year",values="SumOfWght").reset_index().fillna(0)

    Slide_12[f"{ppt12['Curr_Month']}_%"] = round((Slide_12[ppt12["Curr_Month_ful"]]/Summarize_PPT12.iloc[0][ppt12["Curr_Month_1"]])*100,2)

    Slide_12[f"{ppt12['Prev_Month']}_%"] = round((Slide_12[ppt12["Prev_Month_ful"]]/Summarize_PPT12.iloc[0][ppt12["Prev_Month_1"]])*100,2)
    Slide_12["INC_DEC"] = Slide_12[ppt12["Curr_Month_ful"]] - Slide_12[ppt12["Prev_Month_ful"]]

    Slide_12 = Slide_12.sort_values(by = ppt12["Curr_Month_ful"],ascending = False)

    Slide_12 = round(Slide_12,2)
    Slide_12.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT12_1_table.csv", index=False)    
    Logger.log_info("PPT12_1 processed and Saved Successfully")
    

json_file_path = 'input.json'
PPT12(Mom_df,json_file_path,conn)

Logger.log_info("PPT13 starts")

def PPT13(json_file_path,conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt13 = data['ppt13']     

    start_date = ppt13["start_date"] # Change this to the desired start date
    end_date = ppt13["end_date"]

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.*
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                Ppt13 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    Ppt13['dest_zone_type'] = Ppt13.apply(lambda row: 'WC' if row['origin'] == row['dest'] else row['dest_zone'], axis=1)
    Ppt13['normal_cn_date'] = pd.to_datetime(Ppt13['normal_cn_date'])
    Ppt13["Month_year"] = Ppt13['normal_cn_date'].dt.strftime('%b-%Y')
    Total_WghtChgs = Ppt13.groupby("Month_year").agg(
    total_Weight_chgs = ("normal_weight_charges","sum")
    ).reset_index()

    Total_WghtChgs["dest_zone_type"] = "Normal Weight Charges"
    pivot_PPT13 = pd.pivot_table(Ppt13,index = "dest_zone_type",columns = "Month_year",values = "normal_weight_charges",aggfunc = "sum").reset_index()
    pivot_PPT13_Total = pd.pivot_table(Total_WghtChgs,index="dest_zone_type",columns="Month_year",values="total_Weight_chgs").reset_index()
    Curr = ppt13["Curr_Month"]
    Prev = ppt13["Prev_Month"]
    pivot_PPT13 = pd.merge(pivot_PPT13,pivot_PPT13_Total,on = ["dest_zone_type",Curr,Prev],how="outer")
    normal_charge_row = pivot_PPT13[pivot_PPT13['dest_zone_type'] == 'Normal Weight Charges']

    # Extract the Normal_charge values for each month (assuming columns are month-year format)
    normal_charge_values = normal_charge_row.iloc[:, 1:].values.flatten()

    # Define the number of working days for each month in a dictionary
    working_days = {
        Prev: ppt13["working_days_Prev"],  # Number of working days in March
        Curr: ppt13["working_days_Curr"]   # Number of working days in April
    }
    # Calculate daily Normal charge for each month
    daily_normal_charge = normal_charge_values / [working_days[col] for col in normal_charge_row.columns[1:]]
    # Create a new row for 'Daily Normal Charge' and append to the dataframe
    daily_normal_charge_row = ['Daily Normal Charge'] + list(daily_normal_charge)
    pivot_PPT13.loc[len(pivot_PPT13)] = daily_normal_charge_row
    TotalWeigh_2 = pivot_PPT13.iloc[:,-1][4]
    TotalWeigh_1 = pivot_PPT13.iloc[:,1][4]
    
    pivot_PPT13[f"{ppt13['Prev']}_%"] =(pivot_PPT13.iloc[:,-1]/TotalWeigh_2)*100
    pivot_PPT13[f"{ppt13['Curr']}_%"] =(pivot_PPT13.iloc[:,1]/TotalWeigh_1)*100

    pivot_PPT13 = round(pivot_PPT13,2)
    pivot_PPT13["INC_DEC"] = round(pivot_PPT13.iloc[:,2] - pivot_PPT13.iloc[:,1])

    pivot_PPT13.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT13_table.csv", index=False)    
    Logger.log_info("PPT13 processed and Saved Successfully")

    Logger.log_info("PPT13_1 Starts")

    PPT13_1_GROUP = (
    Ppt13.groupby(["Month_year", "master_dest","dest_zone"])['normal_weight_charges']
    .sum()
    .reset_index(name="nW_Total")  # Resetting index and naming the count column
    )
    PPT13_1_GROUP = pd.pivot_table(PPT13_1_GROUP,index=["master_dest","Month_year"],columns="dest_zone",values="nW_Total",aggfunc="sum").reset_index()

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['A'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='A')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(PPT13_1_GROUP)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('A', ppt13['Curr_Month']), ascending=False)
    sorted_top_values[f"Total{ppt13['Curr']}"] = round(sorted_top_values["A"][f"{ppt13['Curr']}-2024"] + sorted_top_values["B"][f"{ppt13['Curr']}-2024"] + sorted_top_values["C"][f"{ppt13['Curr']}-2024"],2)
    sorted_top_values[f"Total{ppt13['Prev']}"] = round(sorted_top_values["A"][f"{ppt13['Prev']}-2024"] + sorted_top_values["B"][f"{ppt13['Prev']}-2024"] + sorted_top_values["C"][f"{ppt13['Prev']}-2024"],2)
    sorted_top_values[f"%_age_{ppt13['Curr']}"] = round((sorted_top_values["TotalMay"]/TotalWeigh_1),2)*100
    sorted_top_values[f"%_age_{ppt13['Prev']}"] = round((sorted_top_values["TotalApr"]/TotalWeigh_2),2)*100
    PPT13_1 = sorted_top_values
    PPT13_1.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT13_1_table.csv", index=False)    
    Logger.log_info("PPT13_1 processed and Saved Successfully")


json_file_path = 'input.json'
PPT13(json_file_path,conn)

def PPT14(Mom_df):
    Logger.log_info("PPT14 STARTS")

    PPT14_GROUP = Mom_df.groupby("month")['n_wght_chgs'].sum().reset_index()
    PPT14_GROUP['month'] = pd.to_datetime(PPT14_GROUP['month'], format='%b-%y')

    # Sort dataframe based on the Month column
    PPT14_GROUP = PPT14_GROUP.sort_values('month')

    # Reformat Month column back to the desired format (e.g., 'Apr-24')
    PPT14_GROUP['month'] = PPT14_GROUP['month'].dt.strftime('%b-%y')
    PPT14_GROUP.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT14_table.csv", index=False)    

    Logger.log_info("PPT14 processed and Finished")
PPT14(Mom_df)

def PPT16(Mom_df):
    Logger.log_info("PPT16 STARTS")
    # Group by 'months' and sum 'n_oth_chgs', then reset the index
    result = Mom_df.groupby("months")["n_oth_chgs"].sum().reset_index()

    # Convert 'months' back to the original string format if needed
    result['months'] = result['months'].dt.strftime('%b-%y')

    result.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT16_table.csv", index=False)
    Logger.log_info("PPT16 saved and processed")
PPT16(Mom_df)

Logger.log_info("PPT17 starts")

def PPT17(json_file_path,conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    Ppt17 = data['ppt17']  

    start_date = Ppt17["start_date"]  # Change this to the desired start date
    end_date = Ppt17["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.*
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                ppt17 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    ppt17['dest_zone_type'] = ppt17.apply(lambda row: 'WC' if row['origin'] == row['dest'] else row['dest_zone'], axis=1)
    ppt17['normal_cn_date'] = pd.to_datetime(ppt17['normal_cn_date'])
    ppt17["Month_year"] = ppt17['normal_cn_date'].dt.strftime('%b-%Y')
    Total_WghtChgs = ppt17.groupby("Month_year").agg(
        total_Weight_chgs = ("normal_revenue","sum")
    ).reset_index()
    Total_WghtChgs["dest_zone_type"] = "normal_revenue"
    pivot_PPT17 = pd.pivot_table(ppt17,index = "dest_zone_type",columns = "Month_year",values = "normal_weight_charges",aggfunc = "sum").reset_index()
    pivot_PPT17_Total = pd.pivot_table(Total_WghtChgs,index="dest_zone_type",columns="Month_year",values="total_Weight_chgs").reset_index()
    Curr = Ppt17["Curr_Month"]
    Prev = Ppt17["Prev_Month"]

    pivot_PPT17 = pd.merge(pivot_PPT17,pivot_PPT17_Total,on = ["dest_zone_type",Curr,Prev],how="outer")
    cod = ppt17[ppt17["cod"]!=0.0]
    Ncod = ppt17[ppt17["cod"]==0.0]
    cod = cod.groupby(["Month_year"])["normal_revenue"].sum().reset_index()
    Ncod = Ncod.groupby(["Month_year"])["normal_revenue"].sum().reset_index()
    cod["dest_zone_type"] = "Total Revenue - COD"
    Ncod["dest_zone_type"] = "Total Revenue - NCOD"
    pivot_cod = pd.pivot_table(cod,index="dest_zone_type",columns="Month_year",values="normal_revenue").reset_index()
    pivot_Ncod = pd.pivot_table(Ncod,index="dest_zone_type",columns="Month_year",values="normal_revenue").reset_index()
    pivot_PPT17 = pd.merge(pivot_PPT17,pivot_cod,on = ["dest_zone_type",Curr,Prev],how="outer")
    pivot_PPT17 = pd.merge(pivot_PPT17,pivot_Ncod,on = ["dest_zone_type",Curr,Prev],how="outer")
    normal_charge_row = pivot_PPT17[pivot_PPT17['dest_zone_type'] == 'normal_revenue']
    normal_charge_values = normal_charge_row.iloc[:, 1:].values.flatten()

    # Define the number of working days for each month in a dictionary
    working_days = {
        Prev: Ppt17["working_days_Prev"],  # Number of working days in March
        Curr: Ppt17["working_days_Curr"]   # Number of working days in April
    }

    # Calculate daily Normal charge for each month
    daily_normal_charge = normal_charge_values / [working_days[col] for col in normal_charge_row.columns[1:]]

    # Create a new row for 'Daily Normal Charge' and append to the dataframe
    daily_normal_charge_row = ['Daily Normal Charge'] + list(daily_normal_charge)
    pivot_PPT17.loc[len(pivot_PPT17)] = daily_normal_charge_row

    TotalWeigh_2 = pivot_PPT17.iloc[:,-1][4]
    TotalWeigh_1 = pivot_PPT17.iloc[:,1][4]


    pivot_PPT17[f"{Ppt17['Curr']}_%"] =(pivot_PPT17.iloc[:,-1]/TotalWeigh_2)*100
    pivot_PPT17[f"{Ppt17['Prev']}_%"] =(pivot_PPT17.iloc[:,1]/TotalWeigh_1)*100

    pivot_PPT17 = round(pivot_PPT17,2)
    pivot_PPT17["INC_DEC"] = round(pivot_PPT17.iloc[:,2] - pivot_PPT17.iloc[:,1])
    pivot_PPT17.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT17_table.csv", index=False)
    Logger.log_info("PPT17 saved and processed")

    Logger.log_info("PPT17_1 starts")

    PPT17_1_GROUP = (
    ppt17.groupby(["Month_year", "master_dest","dest_zone"])['normal_revenue']
    .sum()
    .reset_index(name="nR_Total")  # Resetting index and naming the count column
    )
    PPT17_1_GROUP = pd.pivot_table(PPT17_1_GROUP,index=["master_dest","Month_year"],columns="dest_zone",values="nR_Total",aggfunc="sum").reset_index()

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['A'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='A')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(PPT17_1_GROUP)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('A', Ppt17["Curr_Month"]), ascending=False)

    sorted_top_values[f"Total{Ppt17['Curr']}"] = round(sorted_top_values["A"][f"{Ppt17['Curr']}-2024"] + sorted_top_values["B"][f"{Ppt17['Curr']}-2024"] + sorted_top_values["C"][f"{Ppt17['Curr']}-2024"],2)
    sorted_top_values[f"Total{Ppt17['Prev']}"] = round(sorted_top_values["A"][f"{Ppt17['Prev']}-2024"] + sorted_top_values["B"][f"{Ppt17['Prev']}-2024"] + sorted_top_values["C"][f"{Ppt17['Prev']}-2024"],2)
    sorted_top_values[f"%_age_{Ppt17['Curr']}"] = round((sorted_top_values[f"Total{Ppt17['Curr']}"]/TotalWeigh_1),2)*100
    sorted_top_values[f"%_age_{Ppt17['Prev']}"] = round((sorted_top_values[f"Total{Ppt17['Prev']}"]/TotalWeigh_2),2)*100

    PPT17_1 = sorted_top_values
    PPT17_1.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT17_1_table.csv", index=False)
    Logger.log_info("PPT17_1 saved and processed")



json_file_path = 'input.json'
PPT17(json_file_path,conn)

Logger.log_info("PPT19 starts")

def PPT18(Mom_df):
     
    PPT18_GROUP = Mom_df.groupby("month")['n_rev'].sum().reset_index()

    PPT18_GROUP['month'] = pd.to_datetime(PPT18_GROUP['month'], format='%b-%y')

    # Sort dataframe based on the Month column
    PPT18_GROUP = PPT18_GROUP.sort_values('month')

    # Reformat Month column back to the desired format (e.g., 'Apr-24')
    PPT18_GROUP['month'] = PPT18_GROUP['month'].dt.strftime('%b-%y')
    PPT18_GROUP.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT18_table.csv", index=False)
    Logger.log_info("PPT18 saved and processed")

PPT18(Mom_df)

Logger.log_info("PP19 STARTS")
def PPT19(json_file_path,conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt19 = data['ppt19']  


    start_date =  ppt19["start_date"]  # Change this to the desired start date
    end_date = ppt19["end_date"]

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """select salesreport.master_dest,salesreport.normal_cn_date ,SUM(salesreport.origin=salesreport.dest) AS n_s_wc,
                    SUM(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A') AS n_s_za,
                    SUM(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B') AS n_s_zb,
                    SUM(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C') AS n_s_zc,
                    SUM(IF(salesreport.origin=salesreport.dest and returnreport.return_cn!='', 1, 0)) AS r_s_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A' and returnreport.return_cn!='', 1, 0)) AS r_s_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B' and returnreport.return_cn!='', 1, 0)) AS r_s_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C' and returnreport.return_cn!='', 1, 0)) AS r_s_zc,
                    SUM(IF(salesreport.origin=salesreport.dest, salesreport.weight, 0)) AS n_wght_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A', salesreport.weight, 0)) AS n_wght_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B', salesreport.weight, 0)) AS n_wght_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C', salesreport.weight, 0)) AS n_wght_zc,
                    SUM(IF(salesreport.origin=salesreport.dest and returnreport.return_cn!='', salesreport.weight, 0)) AS r_wght_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A' and returnreport.return_cn!='', salesreport.weight, 0)) AS r_wght_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B' and returnreport.return_cn!='', salesreport.weight, 0)) AS r_wght_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C' and returnreport.return_cn!='', salesreport.weight, 0)) AS r_wght_zc,
                    SUM(IF(salesreport.origin=salesreport.dest, salesreport.normal_weight_charges, 0)) AS n_wght_chgs_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A', salesreport.normal_weight_charges, 0)) AS n_wght_chgs_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B', salesreport.normal_weight_charges, 0)) AS n_wght_chgs_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C', salesreport.normal_weight_charges, 0)) AS n_wght_chgs_zc,
                    SUM(IF(salesreport.origin=salesreport.dest, salesreport.normal_revenue, 0)) AS n_rev_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A', salesreport.normal_revenue, 0)) AS n_rev_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B', salesreport.normal_revenue, 0)) AS n_rev_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C', salesreport.normal_revenue, 0)) AS n_rev_zc,
                    SUM(IF(salesreport.origin=salesreport.dest and returnreport.return_cn!='', returnreport.return_weight_charges, 0)) AS r_rev_wc,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='A' and returnreport.return_cn!='', returnreport.return_weight_charges, 0)) AS r_rev_za,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='B' and returnreport.return_cn!='', returnreport.return_weight_charges, 0)) AS r_rev_zb,
                    SUM(IF(salesreport.origin!=salesreport.dest and salesreport.dest_zone='C' and returnreport.return_cn!='', returnreport.return_weight_charges, 0)) AS r_rev_zc

                    from blueex.salesreport 
                    left join blueex.returnreport 
                    on returnreport.normal_cn = salesreport.normal_cn
                    where salesreport.normal_cn_date between %s and %s
                    group by salesreport.master_dest,salesreport.normal_cn_date
                    order by SUM(salesreport.origin=salesreport.dest) desc;"""
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                Ppt19 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

    Ppt19['normal_cn_date'] = pd.to_datetime(Ppt19['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    Ppt19["Month_year"] = Ppt19['normal_cn_date'].dt.strftime('%b-%Y')
    Total_ret_rev = Ppt19.groupby(["Month_year"])[['r_rev_wc', 'r_rev_za', 'r_rev_zb', 'r_rev_zc']].sum().reset_index()
    Total_ret_rev["Total_Ret_Revenue"] = Total_ret_rev["r_rev_wc"]+Total_ret_rev["r_rev_za"]+Total_ret_rev["r_rev_zb"]+Total_ret_rev["r_rev_zc"]

    Summarize_PPT19 = Ppt19.groupby("Month_year").agg(
    WC = ("r_rev_wc","sum"),
    Zone_A = ('r_rev_za',"sum"),
    Zone_B = ('r_rev_zb',"sum"),
    Zone_C = ('r_rev_zc',"sum"),
    ).reset_index()


    if isinstance(Total_ret_rev["Total_Ret_Revenue"], pd.Series):
        Total_ret_rev = Total_ret_rev["Total_Ret_Revenue"].to_frame()

    # Concatenate along columns
    Summarize_PPT19 = pd.concat([Summarize_PPT19, Total_ret_rev["Total_Ret_Revenue"]], axis=1)

    Summarize_PPT19 = pd.pivot_table(
    Summarize_PPT19,
    values=['WC', 'Zone_A', 'Zone_B', 'Zone_C', 'Total_Ret_Revenue'],
    index=[],
    columns=['Month_year']
    )

    Curr_Total_Rev = Summarize_PPT19.iloc[:,0][0]
    Prev_Total_Rev = Summarize_PPT19.iloc[:,1][0]

    Summarize_PPT19[f"{ppt19['Curr']}_%"] = round((Summarize_PPT19[f"{ppt19['Curr']}-2024"]/Curr_Total_Rev)*100,2)
    Summarize_PPT19[f"{ppt19['Prev']}_%"] = round((Summarize_PPT19[f"{ppt19['Prev']}-2024"]/Prev_Total_Rev)*100,2)
    Summarize_PPT19["INC_DEC"] = Summarize_PPT19.iloc[:,0] - Summarize_PPT19.iloc[:,1]
    Summarize_PPT19.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT19_table.csv", index=False)
    Logger.log_info("PPT19 saved and processed")

    Logger.log_info("PPT19_1 starts")

    PPT19_Group = Ppt19.groupby(["master_dest","Month_year"])[['r_rev_wc', 'r_rev_za', 'r_rev_zb', 'r_rev_zc']].sum().reset_index()
    PPT19_Group["Total_ret_rev"] = PPT19_Group['r_rev_wc'] + PPT19_Group['r_rev_za'] + PPT19_Group['r_rev_zb'] + PPT19_Group['r_rev_zc']
    PPT19_Group = pd.pivot_table(PPT19_Group,index=["master_dest","Month_year"],values="Total_ret_rev",aggfunc="sum").reset_index()
    PPT19_Group = pd.pivot_table(PPT19_Group,index="master_dest",columns="Month_year",values="Total_ret_rev",aggfunc="sum").reset_index()
    PPT19_Group = PPT19_Group.sort_values(by=f"{ppt19['Curr_Month']}",ascending=False).head(10)
    PPT19_Group.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT19_1_table.csv", index=False)
    Logger.log_info("PPT19_1 saved and processed")

json_file_path = 'input.json'
PPT19(json_file_path,conn)

Logger.log_info("PPT21 STARTS")

def PPT21(Mom_df,json_file_path,conn):

    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt20 = data['ppt20']  
    def calc_rps_ex_fsc(n_rev, n_fsc, shipments):
        return (n_rev.sum() - n_fsc.sum()) / shipments.sum()

    def calc_rps_ex_chc(n_rev, n_oth_chgs, shipments):
        return (n_rev.sum() - n_oth_chgs.sum()) / shipments.sum()


    RPS_TOTAL = Mom_df.groupby("month").apply(
        lambda df: pd.Series({
            "RPS": df["n_rev"].sum() / df["shipments"].sum(),
            "RPS_EX_FSC": calc_rps_ex_fsc(df["n_rev"], df["n_fsc"], df["shipments"]),
            "RPS_EX_CHC": calc_rps_ex_chc(df["n_rev"],df["n_oth_chgs"], df["shipments"]),
            "RPS_COD": df[df["cod"] != 0.0]["n_rev"].sum() / df[df["cod"] != 0.0]["shipments"].sum(),
            "RPS_NCOD": df[df["cod"] == 0.0]["n_rev"].sum() / df[df["cod"] == 0.0]["shipments"].sum(),
            "RPS_wc": df["n_rev_wc"].sum() / df["n_s_wc"].sum(),
            "RPS_za": df["n_rev_za"].sum() / df["n_s_za"].sum(),
            "RPS_zb": df["n_rev_zb"].sum() / df["n_s_zb"].sum(),
            "RPS_zc": df["n_rev_zc"].sum() / df["n_s_zc"].sum()
        })
    )

    PPT21_Group = round(RPS_TOTAL.reset_index(),2)
    PPT21_Group.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT21_table.csv", index=False)
    Logger.log_info("PPT21 saved and processed")

    Logger.log_info("PPT21_1 Starts")

    start_date = ppt20["start_date"]  # Change this to the desired start date
    end_date = ppt20["end_date"]

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.*
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                Ppt20 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    Ppt20['normal_cn_date'] = pd.to_datetime(Ppt20['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    Ppt20["Month_year"] = Ppt20['normal_cn_date'].dt.strftime('%b-%Y')

    Ppt20['dest_zone_type'] = Ppt20.apply(lambda row: 'WC' if row['origin'] == row['dest'] else row['dest_zone'], axis=1)

    Grouped_PPT21_1 = Ppt20.groupby(["Month_year","master_dest","dest_zone_type"]).agg(
        Normal_rev = ("normal_revenue","sum"),
        Shipments = ("normal_cn","count")

    ).reset_index()

    Total_Grouped_PPT21_1 = Ppt20.groupby(["Month_year", "master_dest"]).agg(
        Normal_rev=("normal_revenue", "sum"),
        Shipments=("normal_cn", "count")
    ).reset_index()

    # Calculate RPS (Revenue Per Shipment) after aggregation
    Total_Grouped_PPT21_1["RPS"] = Total_Grouped_PPT21_1["Normal_rev"] / Total_Grouped_PPT21_1["Shipments"]

    Grouped_PPT21_1["RPS"] = Grouped_PPT21_1["Normal_rev"]/Grouped_PPT21_1["Shipments"]

    Grouped_PPT21_1 = pd.pivot_table(Grouped_PPT21_1,index=["master_dest","Month_year"],columns="dest_zone_type",values="RPS",aggfunc="sum").reset_index()

    Grouped_PPT21_1 = pd.merge(Total_Grouped_PPT21_1,Grouped_PPT21_1,on = ["master_dest","Month_year"],how = "outer")

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['RPS'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='RPS')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(Grouped_PPT21_1)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C","WC","RPS"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('RPS', f'{ppt20["Curr_Month"]}'), ascending=False)

    PPT21_1 = round(sorted_top_values,2)
    PPT21_1.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT21_1_table.csv", index=False)
    Logger.log_info("PPT21_1 saved and processed")
json_file_path = 'input.json'
PPT21(Mom_df,json_file_path,conn)

Logger.log_info("PPT22 START")

def PPT22(Mom_df):
    Summarize_Mom_Rep = Mom_df.groupby('month').agg(
            Active_Accounts=('acc_no', "count"),
            Normal_Shipments=("shipments", "sum"),
            Normal_Weight=('n_wght', "sum"),
            Weight_Charges=('n_wght_chgs', "sum"),
            Cash_handling=('n_oth_chgs', "sum"),
            fuel_Surcharge=('n_fsc', "sum"),
            Total_normal_rev=("n_rev", "sum")
    ).reset_index()
        
    # Calculations
    Summarize_Mom_Rep["RPS"] = round(Summarize_Mom_Rep["Total_normal_rev"] / Summarize_Mom_Rep["Normal_Shipments"], 2)
    Ppt22 = Summarize_Mom_Rep[["month","RPS"]]
    Ppt22.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT22_table.csv", index=False)
    Logger.log_info("PPT22 saved and processed")
PPT22(Mom_df)

Logger.log_info("PPT_24 STARTS")

def PPT24(Mom_df,json_file_path,conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt24 = data['ppt24']  
    def calc_rps_ex_fsc(n_rev, n_fsc, n_wght):
        return (n_rev.sum() - n_fsc.sum()) / n_wght.sum()

    def calc_rps_ex_chc(n_rev, n_oth_chgs, n_wght):
        return (n_rev.sum() - n_oth_chgs.sum()) / n_wght.sum()

    RPS_TOTAL = Mom_df.groupby("month").apply(
        lambda df: pd.Series({
            "RPW": df["n_rev"].sum() / df["n_wght"].sum(),
            "RPW_EX_FSC": calc_rps_ex_fsc(df["n_rev"], df["n_fsc"], df["n_wght"]),
            "RPW_EX_CHC": calc_rps_ex_chc(df["n_rev"], df["n_oth_chgs"], df["n_wght"]),
            "RPW_wc": df["n_rev_wc"].sum() / df["n_wght_wc"].sum(),
            "RPW_za": df["n_rev_za"].sum() / df["n_wght_za"].sum(),
            "RPW_zb": df["n_rev_zb"].sum() / df["n_wght_zb"].sum(),
            "RPW_zc": df["n_rev_zc"].sum() / df["n_wght_zc"].sum()  # Assuming you meant to use n_wght_zc
        })
    )

    PPT24_Group = round(RPS_TOTAL.reset_index(), 2)
    PPT24_Group.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT24_table.csv", index=False)
    Logger.log_info("PPT24 saved and processed")

    Logger.log_info("PPT24_1 starts")

    start_date = ppt24["start_date"]  # Change this to the desired start date
    end_date = ppt24["end_date"]


    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.*
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                Ppt24 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    Ppt24['normal_cn_date'] = pd.to_datetime(Ppt24['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    Ppt24["Month_year"] = Ppt24['normal_cn_date'].dt.strftime('%b-%Y')

    Ppt24['dest_zone_type'] = Ppt24.apply(lambda row: 'WC' if row['origin'] == row['dest'] else row['dest_zone'], axis=1)

    Grouped_PPT24_1 = Ppt24.groupby(["Month_year","master_dest","dest_zone_type"]).agg(
        Normal_rev = ("normal_revenue","sum"),
        Wght=("weight", "sum")

    ).reset_index()

    Total_Grouped_PPT24_1 = Ppt24.groupby(["Month_year", "master_dest"]).agg(
        Normal_rev=("normal_revenue", "sum"),
        Wght=("weight", "sum")
    ).reset_index()

    # Calculate RPS (Revenue Per Shipment) after aggregation
    Total_Grouped_PPT24_1["RPW"] = Total_Grouped_PPT24_1["Normal_rev"] / Total_Grouped_PPT24_1["Wght"]

    Grouped_PPT24_1["RPW"] = Grouped_PPT24_1["Normal_rev"]/Grouped_PPT24_1["Wght"]

    Grouped_PPT24_1 = pd.pivot_table(Grouped_PPT24_1,index=["master_dest","Month_year"],columns="dest_zone_type",values="RPW",aggfunc="sum").reset_index()

    Grouped_PPT24_1 = pd.merge(Total_Grouped_PPT24_1,Grouped_PPT24_1,on = ["master_dest","Month_year"],how = "outer")

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['RPW'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='RPW')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(Grouped_PPT24_1)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C","WC","RPW"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('RPW', f'{ppt24["Curr_Month"]}'), ascending=False)

    PPT24_1 = round(sorted_top_values,2)
    PPT24_1.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT24_1_table.csv", index=False)
    Logger.log_info("PPT24_1 saved and processed")
json_file_path = 'input.json'
PPT24(Mom_df,json_file_path,conn)

Logger.log_info("PPT25 starts")

def PPT25(Mom_df):
    Summarize_Mom_Rep = Mom_df.groupby('month').agg(
            Active_Accounts=('acc_no', "count"),
            Normal_Shipments=("shipments", "sum"),
            Normal_Weight=('n_wght', "sum"),
            Weight_Charges=('n_wght_chgs', "sum"),
            Cash_handling=('n_oth_chgs', "sum"),
            fuel_Surcharge=('n_fsc', "sum"),
            Total_normal_rev=("n_rev", "sum")
        ).reset_index()
        
        # Calculations
    Summarize_Mom_Rep["RPW"] = round(Summarize_Mom_Rep["Total_normal_rev"] / Summarize_Mom_Rep["Normal_Weight"], 2)
    Ppt25 = Summarize_Mom_Rep[["month","RPW"]]
    Ppt25.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Ppt25_table.csv", index=False)
    Logger.log_info("Ppt25 saved and processed")
PPT25(Mom_df)

def PPT26(Mom_df,json_file_path,conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file) 
    ppt26 = data['ppt26']  
    RPS_TOTAL = Mom_df.groupby("month").apply(
    lambda df: pd.Series({
        "W/S": df["n_wght"].sum() / df["shipments"].sum(),
        "W/S_wc": df["n_wght_wc"].sum() / df["n_s_wc"].sum(),
        "W/S_za": df["n_wght_za"].sum() / df["n_s_za"].sum(),
        "W/S_zb": df["n_wght_zb"].sum() / df["n_s_zb"].sum(),
        "W/S_zc": df["n_wght_zc"].sum() / df["n_s_zc"].sum()  # Assuming you meant to use n_wght_zc
        })
    )

    PPT26_Group = round(RPS_TOTAL.reset_index(), 2)
    PPT26_Group.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Ppt26_table.csv", index=False)
    Logger.log_info("Ppt26 saved and processed")

    Logger.log_info("Ppt26_1 saved and processed")

    start_date = ppt26["start_date"]  # Change this to the desired start date
    end_date = ppt26["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 =  """SELECT sr.*
                        FROM blueex.salesreport sr 
                        WHERE sr.normal_cn_date BETWEEN %s AND %s                 
                    """
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                PPT26_1 = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    PPT26_1['normal_cn_date'] = pd.to_datetime(PPT26_1['normal_cn_date'])

    # Convert datetime to string with specified format '%b-%Y'
    PPT26_1["Month_year"] = PPT26_1['normal_cn_date'].dt.strftime('%b-%Y')

    PPT26_1['dest_zone_type'] = PPT26_1.apply(lambda row: 'WC' if row['origin'] == row['dest'] else row['dest_zone'], axis=1)

    Grouped_PPT26_1 = PPT26_1.groupby(["Month_year","master_dest","dest_zone_type"]).agg(
        Wght = ("weight","sum"),
        Shipments = ("normal_cn","count")

    ).reset_index()

    Total_Grouped_PPT26_1 = PPT26_1.groupby(["Month_year", "master_dest"]).agg(
        Wght=("weight", "sum"),
        Shipments=("normal_cn", "count")
    ).reset_index()

    # Calculate RPS (Revenue Per Shipment) after aggregation
    Total_Grouped_PPT26_1["W/S"] = Total_Grouped_PPT26_1["Wght"] / Total_Grouped_PPT26_1["Shipments"]

    Grouped_PPT26_1["W/S"] = Grouped_PPT26_1["Wght"]/Grouped_PPT26_1["Shipments"]

    Grouped_PPT26_1 = pd.pivot_table(Grouped_PPT26_1,index=["master_dest","Month_year"],columns="dest_zone_type",values="W/S",aggfunc="sum").reset_index()

    Grouped_PPT26_1 = pd.merge(Total_Grouped_PPT26_1,Grouped_PPT26_1,on = ["master_dest","Month_year"],how = "outer")

    def top_10_based_on_A_per_month(df):
        # Initialize an empty DataFrame to store the results
        top_values_per_month = pd.DataFrame(columns=df.columns)
        
        # Iterate over unique months in the DataFrame
        for month in df['Month_year'].unique():
            # Filter DataFrame for the current month
            df_month = df[df['Month_year'] == month]
            
            # Filter out rows where column 'A' is null or NaN within the current month
            df_filtered = df_month.dropna(subset=['W/S'])
            
            # Sort DataFrame by column 'A' in descending order and get top 10 rows
            top_10 = df_filtered.nlargest(10, columns='W/S')
            
            # Append the top 10 rows for the current month to the results DataFrame
            top_values_per_month = pd.concat([top_values_per_month, top_10], ignore_index=True)
        
        return top_values_per_month

    # Call the function to find top 10 based on column 'A' per month
    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month(Grouped_PPT26_1)

    top_10_based_on_A_per_month_values = top_10_based_on_A_per_month_values.sort_values(by='A', ascending=False)
    top_10_based_on_A_per_month_values = pd.pivot_table(top_10_based_on_A_per_month_values,index="master_dest",columns=["Month_year"],values=["A","B","C","WC","W/S"]).fillna(0).reset_index()   
    sorted_top_values = top_10_based_on_A_per_month_values.sort_values(by=('W/S', f'{ppt26["Curr_Month"]}'), ascending=False)

    PPT26_1 = round(sorted_top_values,2)
    PPT26_1.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\PPT26_1_table.csv", index=False)
    Logger.log_info("PPT26_1 saved and processed")

json_file_path = 'input.json'
PPT26(Mom_df,json_file_path,conn)

Logger.log_info("PPT27 starts")

def PPT27(Mom_df):
    Summarize_Mom_Rep = Mom_df.groupby('month').agg(
            Active_Accounts=('acc_no', "count"),
            Normal_Shipments=("shipments", "sum"),
            Normal_Weight=('n_wght', "sum"),
            Weight_Charges=('n_wght_chgs', "sum"),
            Cash_handling=('n_oth_chgs', "sum"),
            fuel_Surcharge=('n_fsc', "sum"),
            Total_normal_rev=("n_rev", "sum")
        ).reset_index()
        
        # Calculations
    Summarize_Mom_Rep["WPS"] = round(Summarize_Mom_Rep["Normal_Weight"] / Summarize_Mom_Rep["Normal_Shipments"], 2)
    Ppt27 = Summarize_Mom_Rep[["month","WPS"]]
    Ppt27.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Ppt27_table.csv", index=False)
    Logger.log_info("Ppt27 saved and processed")
PPT27(Mom_df)

Logger.log_info("PPT28 STARTS")

def PPT28(Mom_df):
    bins = [0, 50, 100, 500, 1000, 1500, 2000, 2500, 3000, 3500, 4000, 4500, 5000, 10000, 15000, 20000]
    labels = ['0-50', '50-100', '100-500', '500-1000', '1000-1500', '1500-2000', '2000-2500', '2500-3000',
            '3000-3500', '3500-4000', '4000-4500', '4500-5000', '5000-10000', '10000-15000', '15000-20000']

    Mom_df['Shipment_Band'] = pd.cut(Mom_df['shipments'], bins=bins, labels=labels, right=False)

    # Group by shipment bands
    grouped = Mom_df.groupby('Shipment_Band').agg(
        Customers_Number=('acc_no', 'count'),
        Customers_Percentage=('acc_no', lambda x: f"{(x.count() / Mom_df['acc_no'].count()) * 100:.2f}%"),
        Shipments_Number=('shipments', 'sum'),
        Shipments_Percentage=('shipments', lambda x: f"{(x.sum() / Mom_df['shipments'].sum()) * 100:.2f}%"),
        Weight_Number=('n_wght', 'sum'),
        Weight_Percentage=('n_wght', lambda x: f"{(x.sum() / Mom_df['n_wght'].sum()) * 100:.2f}%"),
        Revenue_Number=('n_rev', 'sum'),
        Revenue_Percentage=('n_rev', lambda x: f"{(x.sum() / Mom_df['n_rev'].sum()) * 100:.2f}%"),
        RPS=('n_rev', lambda x: (x.sum() / len(x) if len(x) > 0 else 0)),
        RPW=('n_rev', lambda x: (x.sum() / Mom_df.loc[x.index, 'n_wght'].sum() if Mom_df.loc[x.index, 'n_wght'].sum() > 0 else 0)),
        WS=('n_wght', lambda x: (x.sum() / len(x) if len(x) > 0 else 0))
    ).reset_index()

    # Format the output as required
    grouped.columns = ['From-To', 'Customers Number', 'Customers %age', 'Shipments Number', 'Shipments %age',
                    'Weight Number', 'Weight %age', 'Revenue Number', 'Revenue %age', 'RPS', 'RPW', 'W/S']
    grouped.to_csv(r"C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Ppt28_table.csv", index=False)
    Logger.log_info("Ppt28 saved and processed")
PPT28(Mom_df) 

Logger.log_info("PPT29 Starts")


def PPT29(json_file_path, conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    ppt29 = data['ppt29']
    # Date range parameters
    start_date = ppt29["start_date"]  # Change this to the desired start date
    end_date = ppt29["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')

    try:
        # Create connection to MySQL database
        with conn.cursor() as cursor:
            # Check database connection
            cursor.execute("SELECT DATABASE();")
            record = cursor.fetchone()
            print("Connected to Database:", record)

            # Define SQL query to fetch records within the date range
            q1 = """
                SELECT
                salesreport.acc_no,
                crf.cust_name as Customer_Name,
                DATE_FORMAT(salesreport.normal_cn_date, '%b-%y') AS month, 
                COUNT(salesreport.normal_cn) AS normal_shipments,
                SUM(salesreport.weight) AS normal_weight,
                SUM(salesreport.normal_revenue) AS total_normal_revenue
                FROM blueex.salesreport
                JOIN crf ON salesreport.acc_no = crf.acc_no
                WHERE salesreport.acc_no IN ('KHI-00344', 'ISB-01009', 'KHI-06962', 'LHE-02620', 
                'MUX-00130', 'PEW-00170', 'PEW-00249', 'KHI-09334', 'KHI-04324', 
                'KHI-04211', 'KHI-04536', 'KHI-04535', 'KHI-04731', 'KHI-10060') 
                AND normal_cn_date BETWEEN %s AND %s
                GROUP BY acc_no, DATE_FORMAT(salesreport.normal_cn_date, '%b-%y');
            """

            # Execute SQL query with parameters
            cursor.execute(q1, (start_date_str, end_date_str))

            # Fetch all rows
            rows = cursor.fetchall()

            # Create DataFrame from fetched rows
            df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

            # Convert 'month' column to datetime format
            df['months'] = pd.to_datetime(df['month'], format='%b-%y', errors='coerce')

            # Filter DataFrame based on date range
            Ppt29 = df[df['months'].between(pd.to_datetime(start_date_str), pd.to_datetime(end_date_str))]

            # Write filtered DataFrame to a CSV file
            Ppt29.to_csv("SalesReportPPT29till31.csv", index=False)

            print(f"MOM HISTORY Records fetched and written to CSV: {len(Ppt29)}")

    except msql.Error as e:
        print(f"Error occurred while connecting to MySQL: {e}")
        conn.rollback()  # Rollback any changes in case of error
        sys.exit()

    df = Ppt29.copy()

    print(df.columns)

    # Print unique customer names to verify
    print("Unique Customer Names:", df['Customer_Name'].unique())

    # List of customer names to process
    customers = {
        'ATLAS HONDA LIMITED': 'AtlasHonda.csv',
        'BAGALLERY (PRIVATE) LIMITED': 'Bagallery.csv',
        'Ideas (Pvt.) Ltd': 'Ideas.csv',
        'M2 (Private) Limited [Ego Brand]': 'M2PVT_LTD.csv',
        'ALMIRAH - U I GARMENTS (PVT) LTD': 'Almira.csv',
        'J. ( Junaid Jamshed )': 'Junaid.csv'
    }

    # Special case for BAT customers
    bat_customers = [
        'Allied Marketing (Pvt) Ltd  // BAT',
        'Pak Distributors // BAT',
        'Khattak Enterprises // BAT',
        'Al-Barka  Trading // BAT'
    ]

    def process_customer_data(df, customer_name, output_file):
        customer_df = df[df['Customer_Name'] == customer_name]
        print(f"\nProcessing data for: {customer_name}")
        print(f"Filtered Data for {customer_name}:\n", customer_df)
        if customer_df.empty:
            print(f"No data found for {customer_name}")
            return

        grouped = customer_df.groupby('month').agg(
            Total_Shipments=('normal_shipments', 'sum'),
            Total_Weight=('normal_weight', 'sum'),
            Total_Revenue=('total_normal_revenue', 'sum')
        ).reset_index()

        grouped['RPS'] = round(grouped['Total_Revenue'] / grouped['Total_Shipments'], 2)
        grouped['RPW'] = round(grouped['Total_Revenue'] / grouped['Total_Weight'], 2)
        grouped['WS'] = round(grouped['Total_Weight'] / grouped['Total_Shipments'], 2)

        pivot_grouped = grouped.pivot_table(values=['Total_Shipments', 'Total_Weight', 'Total_Revenue', 'RPS', 'RPW', 'WS'],
                                            columns='month').fillna(0).reset_index()
        pivot_grouped.rename(columns={'index': 'Description'}, inplace=True)

        columns_to_sort = [col for col in pivot_grouped.columns if col != 'Description']
        sorted_columns = sorted(columns_to_sort, key=lambda x: pd.to_datetime(x, format='%b-%y'))

        # Reorder columns based on sorted column names
        pivot_grouped = pivot_grouped[['Description'] + sorted_columns]

        Description_order = ["Total_Shipments", "Total_Weight", "Total_Revenue", "RPS", "RPW", "WS"]

        # Convert 'Description' column to Categorical with specified order
        pivot_grouped['Description'] = pd.Categorical(pivot_grouped['Description'], categories=Description_order,
                                                      ordered=True)

        # Sort DataFrame by the custom order of 'Description'
        pivot_grouped = pivot_grouped.sort_values(by='Description')

        # Print the rearranged dataframe
        print(pivot_grouped.to_string(index=False))
        pivot_grouped.to_csv(output_file, index=False)


    # Process each customer
    for customer, output_file in customers.items():
        process_customer_data(df, customer, output_file)

    # Process BAT customers
    bat_df = df[df['Customer_Name'].isin(bat_customers)]
    print("\nFiltered Data for BAT customers:\n", bat_df)
    if not bat_df.empty:
        grouped = bat_df.groupby('month').agg(
            Total_Shipments=('normal_shipments', 'sum'),
            Total_Weight=('normal_weight', 'sum'),
            Total_Revenue=('total_normal_revenue', 'sum')
        ).reset_index()

        grouped['RPS'] = round(grouped['Total_Revenue'] / grouped['Total_Shipments'], 2)
        grouped['RPW'] = round(grouped['Total_Revenue'] / grouped['Total_Weight'], 2)
        grouped['WS'] = round(grouped['Total_Weight'] / grouped['Total_Shipments'], 2)

        pivot_grouped = grouped.pivot_table(values=['Total_Shipments', 'Total_Weight', 'Total_Revenue', 'RPS', 'RPW', 'WS'],
                                            columns='month').fillna(0).reset_index()
        pivot_grouped.rename(columns={'index': 'Description'}, inplace=True)

        columns_to_sort = [col for col in pivot_grouped.columns if col != 'Description']
        sorted_columns = sorted(columns_to_sort, key=lambda x: pd.to_datetime(x, format='%b-%y'))

        # Reorder columns based on sorted column names
        pivot_grouped = pivot_grouped[['Description'] + sorted_columns]

        Description_order = ["Total_Shipments", "Total_Weight", "Total_Revenue", "RPS", "RPW", "WS"]

        # Convert 'Description' column to Categorical with specified order
        pivot_grouped['Description'] = pd.Categorical(pivot_grouped['Description'], categories=Description_order,
                                                      ordered=True)

        # Sort DataFrame by the custom order of 'Description'
        pivot_grouped = pivot_grouped.sort_values(by='Description')

        # Print the rearranged dataframe
        print(pivot_grouped.to_string(index=False))
        pivot_grouped.to_csv('BAT.csv', index=False)
    else:
        print("No data found for BAT customers")

    Logger.log_info("PPT29 - PPT32 ends")

json_file_path = 'input.json'
PPT29(json_file_path, conn)


Logger.log_info("PPT33 - PPT35 Starts")

def PPT30(json_file_path, conn):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    ppt30 = data['ppt30']
    c_start_date = ppt30["start_date"]  # Change this to the desired start date
    c_end_date = ppt30["end_date"]    # Change this to the desired end date


    # Convert string dates to datetime objects
    start_date = datetime.strptime(c_start_date, "%Y-%m-%d")
    end_date = datetime.strptime(c_end_date, "%Y-%m-%d")

    # Creating Six Months Period
    p1 = start_date.strftime('%b-%y')
    p2 = end_date.strftime('%b-%y')


    try:
        # Create ExcelWriter object to write to Excel file
            with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 = f"SELECT * FROM blueex.mom_history"

                # Execute SQL query
                cursor.execute(q1)

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                df = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])

                # Convert 'month' column to datetime format
                df['months'] = pd.to_datetime(df['month'], format='%b-%y', errors='coerce')

                # Filter DataFrame based on date range
                filtered_df = df[df['months'].between(pd.to_datetime(p1, format='%b-%y', errors='coerce'),
                                                    pd.to_datetime(p2, format='%b-%y', errors='coerce'))]

                # Write filtered DataFrame to a new sheet in the Excel file
                sheet_name = "MOM HISTORY DATA"
                # filtered_df.to_excel(writer, sheet_name=sheet_name, index=False)

                print(f"MOM HISTORY Records fetched and written to Excel sheet: '{sheet_name}'")
                print("MOM HISTORY Records fetched and written to CSV:", len(filtered_df))
                


    except msql.Error as e:
        print(f"Error occurred while connecting to MySQL: {e}")
        conn.rollback()  # Rollback any changes in case of error
        sys.exit()


    df=filtered_df.copy()

    df['RPS'] = round(df['n_rev'] / df['shipments'],2)
    df['RPW'] = round(df['n_rev'] / df['n_wght'],2)
    df['WS'] = round(df['n_wght'] / df['shipments'],2)
    df['Return Ratio'] = (df['ret_ship'] / df['shipments']) * 100
    df['Return Ratio (WC + ZA)'] = ((df['r_s_wc'] + df['r_s_za']) / df['shipments']) * 100
    df['WC + ZA'] = ((df['n_s_wc'] + df['n_s_za']) / df['shipments']) * 100
    df['0_1_Kg'] = ((df['kg_0_0_5'] + df['kg_0_5_1_0']) / df['shipments']) * 100
    df['Revenue %'] = (df['n_rev'] / df['n_rev'].sum()) * 100
    df['Return Tariff'] = round((df['ret_revenue'] / df['ret_ship']),0)
    df['ZB'] = (df['n_s_zb'] / df['shipments'])*100

    filtered_df = df[(df['shipments'] >= 100) & (df['WS'] >= 0.5)]

    # Sort by 'shipments' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='shipments', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5Shipments.csv')


    # Sort by 'Revenue' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='n_rev', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5Revenue.csv')


    # Sort by 'RPS' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='RPS', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5RPS.csv')



    # Sort by 'RPW' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='RPW', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5RPW.csv')


    # Sort by 'Lowest RPS' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='RPS', ascending=True).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5LowestRPS.csv')


    # Sort by 'Lowest RPW' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='RPW', ascending=True).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5LowestRPW.csv')


    # Sort by 'Highest Return Ratio' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='Return Ratio', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'WC + ZA', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'WC + ZA', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['WC + ZA'] = top_5_customers['WC + ZA'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5HighestReturnRatio.csv')


    # Sort by 'Highest ZB Shipemnts' and get the top 5 customers
    top_5_customers = filtered_df.sort_values(by='ZB', ascending=False).head(5)

    # Select and rename the required columns
    top_5_customers = top_5_customers[['cust_name', 'sp_name', 'shipments', 'ZB', '0_1_Kg', 'n_wght', 'n_rev', 'Revenue %', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'ret_ship', 'Return Ratio', 'Return Ratio (WC + ZA)']]
    top_5_customers.columns = ['Customer', 'SP', 'Total Shipments', 'ZB', '0_1_Kg', 'Weight', 'Revenue', '%age of Revenue', 'RPS', 'RPW', 'WS', 'Return Tariff' , 'Return Shipments', 'Return Ratio', 'Return Ratio (WC + ZA)']

    # Format the numbers for better readability
    top_5_customers['Total Shipments'] = top_5_customers['Total Shipments'].apply(lambda x: f"{x:,}")
    top_5_customers['Weight'] = top_5_customers['Weight'].apply(lambda x: f"{x:,}")
    top_5_customers['Revenue'] = top_5_customers['Revenue'].apply(lambda x: f"{x:,}")
    top_5_customers['%age of Revenue'] = top_5_customers['%age of Revenue'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['ZB'] = top_5_customers['ZB'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['0_1_Kg'] = top_5_customers['0_1_Kg'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio'] = top_5_customers['Return Ratio'].apply(lambda x: f"{x:.2f}%")
    top_5_customers['Return Ratio (WC + ZA)'] = top_5_customers['Return Ratio (WC + ZA)'].apply(lambda x: f"{x:.2f}%")

    top_5_customers.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT\Top5ZBShipments.csv')

    Logger.log_info("PPT33-35 COMPLETES")

json_file_path = 'input.json'
PPT30(json_file_path, conn)

Logger.log_info("Presentation_2 INDIDUAL DOMESTIC START")

Logger.log_info("ALL_DATA Presentation_2")

def all_Data(Mom_df,json_file_path):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    ppt1_AllData = data['All_data']
    Mom_df = Mom_df[Mom_df["month"] == ppt1_AllData["Curr_Month"]]
    Normal_shipment = Mom_df[["month","cust_type","acc_no","sp_name","shipments","n_wght","n_wght_chgs","n_oth_chgs","n_rev","n_s_za","n_fsc"]]
    Normal_shipment["Rev_Wi_FSC"] = Normal_shipment["n_rev"] - Normal_shipment["n_fsc"]
    Normal_shipment["Type"] = "Normal"

    Normal_shipment.columns = ["Month","Customer Type","Account No","Sales Person","shipments","Weight","Weight Charges","Other Charges","Revenue","Shipment Zone A","FSC","Revenue Without FSC","Type"]
    Ret_Normal_shipment = Mom_df[["month","cust_type","acc_no","sp_name","ret_ship","r_wght","r_wght_chrg","ret_revenue","r_s_za"]]
    Ret_Normal_shipment = Ret_Normal_shipment[Ret_Normal_shipment["ret_ship"]!=0]
    Ret_Normal_shipment["Other Charges"] = 0
    Ret_Normal_shipment["FSC"]  = 0
    Ret_Normal_shipment["Rev_Wi_FSC"] = Ret_Normal_shipment["ret_revenue"]
    Ret_Normal_shipment["Type"] = "Return"
    Ret_Normal_shipment = Ret_Normal_shipment[["month","cust_type","acc_no","sp_name","ret_ship","r_wght","r_wght_chrg","Other Charges","ret_revenue","r_s_za","FSC","Rev_Wi_FSC","Type"]]
    Ret_Normal_shipment.columns = ["Month","Customer Type","Account No","Sales Person","shipments","Weight","Weight Charges","Other Charges","Revenue","Shipment Zone A","FSC","Revenue Without FSC","Type"]
    All_Data = pd.merge(Normal_shipment,Ret_Normal_shipment,on = ['Month', 'Customer Type', 'Account No', 'Sales Person', 'shipments',
       'Weight', 'Weight Charges', 'Other Charges', 'Revenue',
       'Shipment Zone A', 'FSC', 'Revenue Without FSC', 'Type'],how="outer")
    
    All_Data.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\ALL_DATA Presentation_2.csv')

    Logger.log_info("ALL_DATA Presentation_2 Completed")
json_file_path = 'input.json'
all_Data(Mom_df,json_file_path)

Logger.log_info("Accounts Tab Second Ppt Starts")

def Account(json_file_path,conn,Mom_df):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    AccountTab = data['AccountTab']
    def query(start_date, end_date):
        conn = msql.connect(host=dlogin.host, user=dlogin.user, password=dlogin.password, database=dlogin.database)
        try:
            with conn.cursor() as cursor:
                sql_query = """
                    SELECT sr.sp_name, COUNT(DISTINCT af.acc_no) AS Distinct_count, %s AS Month
                    FROM blueex.acc_form af
                    INNER JOIN salesreport sr ON af.acc_no = sr.acc_no
                    WHERE af.finance_approval BETWEEN %s AND %s
                    AND af.second_approval BETWEEN %s AND %s
                    GROUP BY sr.sp_name;
                """
                cursor.execute(sql_query, (datetime.strptime(start_date, "%Y-%m-%d").strftime('%b-%Y'), start_date, end_date, start_date, end_date))
                data = cursor.fetchall()
                columns = [desc[0] for desc in cursor.description]
                month_df = pd.DataFrame(data, columns=columns)
                return month_df
        finally:
            conn.close()

    # Define the month ranges
    month_ranges = AccountTab["month_ranges"]

    # List to store individual DataFrames for each month
    dfs = []

    # Loop through the month ranges
    for start_date, end_date in month_ranges:
        df = query(start_date, end_date)
        dfs.append(df)
        print(dfs)

    # Concatenate all DataFrames into a single DataFrame
    merged_df = pd.concat(dfs, ignore_index=True)
    Approved_Account = pd.pivot_table(merged_df,index="sp_name",columns="Month",values="Distinct_count").reset_index().fillna(0)
    month_columns = [col for col in Approved_Account.columns if col != 'sp_name']
    sorted_month_columns = sorted(month_columns, key=lambda x: pd.to_datetime(x, format="%b-%Y"))  # Adjust the format as per your month format
    Approved_Account = Approved_Account[['sp_name'] + sorted_month_columns]

    Approved_Account.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\Approved_Account Presentation_2.csv')

    Logger.log_info("Approved_Account Presentation_2 Completed")
    
    Logger.log_info("New ACTIVE oF aCOOUNT TAB STARTED")

    start_date = AccountTab["start_date"]  # Change this to the desired start date
    end_date = AccountTab["end_date"]    # Change this to the desired end date

    start_datetime = datetime.strptime(start_date, '%Y-%m-%d')
    end_datetime = datetime.strptime(end_date, '%Y-%m-%d')

    start_date_str = start_datetime.strftime('%Y-%m-%d')
    end_date_str = end_datetime.strftime('%Y-%m-%d')
    with conn.cursor() as cursor:
                # Check database connection
                cursor.execute("SELECT DATABASE();")
                record = cursor.fetchone()
                print("Connected to Database:", record)

                # Define SQL query to fetch records within the date range
                q1 = "SELECT * from blueex.salesreport sr LEFT JOIN acc_form af ON sr.acc_no = af.acc_no   WHERE sr.normal_cn_date  BETWEEN %s AND %s "
                
                # Execute SQL query
                cursor.execute(q1, (start_date, end_date))

                # Fetch all rows
                rows = cursor.fetchall()

                # Create DataFrame from fetched rows
                NewActive = pd.DataFrame(rows, columns=[i[0] for i in cursor.description])
    NewActive['normal_cn_date'] = pd.to_datetime(NewActive['normal_cn_date'], errors='coerce')
    NewActive['first_cn'] = pd.to_datetime(NewActive['first_cn'], errors='coerce')

    # Drop rows with NaT in date columns
    NewActive.dropna(subset=['normal_cn_date', 'first_cn'], inplace=True)

    # Extract month and year
    NewActive['normal_cn_year_month'] = NewActive['normal_cn_date'].dt.to_period('M')
    NewActive['first_cn_year_month'] = NewActive['first_cn'].dt.to_period('M')

    # Check if the year and month are the same
    NewActive['same_month_year'] = NewActive['normal_cn_year_month'] == NewActive['first_cn_year_month']

    # Filter rows where the condition is met
    filtered_df = NewActive[NewActive['same_month_year']]

    # Get unique months from normal_cn_date
    unique_months = NewActive['normal_cn_year_month'].unique()

    # Initialize a list to hold results
    all_results = []

    # Loop through each unique month and count unique acc_no for each sp_name
    for month in unique_months:
        monthly_data = filtered_df[filtered_df['normal_cn_year_month'] == month]
        result = monthly_data.groupby('sp_name')['acc_no'].nunique().reset_index()
        result['month'] = month  # Add the month column to the result
        all_results.append(result)

    # Combine all results into a single DataFrame
    final_result = pd.concat(all_results, ignore_index=True)

    # Rename columns for clarity
    final_result.columns = ["SalesPerson", "Account_No","Account_no_1", "month"]

    # Create a pivot table
    final_result = pd.pivot_table(final_result, index="SalesPerson", columns="month", values="Account_No").reset_index().fillna(0)

    
    NewActive.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\NewActive Presentation_2.csv')

    Logger.log_info("New_Active Presentation_2 Completed")

    Logger.log_info("Account Lost starts")

    acc_lost = NewActive.copy()
    duplicate_columns = acc_lost.columns[acc_lost.columns.duplicated()]
    acc_lost = acc_lost.loc[:, ~acc_lost.columns.duplicated()]

    acc_lost['normal_cn_date'] = pd.to_datetime(acc_lost['normal_cn_date'])

    # Extract year and month in 'YYYY-MM' format
    acc_lost['normal_cn_year_month'] = acc_lost['normal_cn_date'].dt.to_period('M')

    # Define the period of interest
    start_period = AccountTab["PrevRange"]
    end_period = AccountTab["CurrRange"]

    # Filter DataFrame for the period of interest
    acc_lost = acc_lost[(acc_lost['normal_cn_year_month'] >= start_period) & (acc_lost['normal_cn_year_month'] <= end_period)]

    # Get unique months sorted
    unique_months = sorted(acc_lost['normal_cn_year_month'].unique())

    # Initialize a list to hold results
    all_results = []

    # Loop through each month starting from the second one
    for i in range(1, len(unique_months)):
        current_month = unique_months[i]
        previous_month = unique_months[i - 1]

        # Filter data for the current and previous months
        current_month_data = acc_lost[acc_lost['normal_cn_year_month'] == current_month]
        previous_month_data = acc_lost[acc_lost['normal_cn_year_month'] == previous_month]

        # Calculate lost accounts (present in previous month but not in current month)
        for sp_name in previous_month_data['sp_name'].unique():
            prev_accounts = previous_month_data[previous_month_data['sp_name'] == sp_name]['acc_no']
            curr_accounts = current_month_data[current_month_data['sp_name'] == sp_name]['acc_no']
            lost_accounts = prev_accounts[~prev_accounts.isin(curr_accounts)]
            lost_count = lost_accounts.nunique()
            all_results.append({'sp_name': sp_name, 'unique_lost_acc_no_count': lost_count, 'month': current_month})

    # Convert results to DataFrame
    final_result_acc = pd.DataFrame(all_results)
    final_result_acc = pd.pivot_table(final_result_acc,index="sp_name",columns="month",values="unique_lost_acc_no_count").reset_index().fillna(0)

    cols = final_result_acc.columns
    cols = cols.to_list()

    for i in range(len(cols)):
        cols[i] = str(cols[i])

    final_result_acc.columns = cols

    final_result_acc.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\acc_lost Presentation_2.csv')

    Logger.log_info("Account Lost Presentation_2 Completed")

json_file_path = 'input.json'
Account(json_file_path,conn,Mom_df)

Logger.log_info("SPWISE SECOND REPORT STARTS")

def SPWISE(Mom_df):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    SP_wise = data['SPWISE']
    Domestic_Second_ppt_spwise = Mom_df[Mom_df["month"] == SP_wise["CurrMonth"]]
    agg_data = Domestic_Second_ppt_spwise.groupby(["sp_name", "cust_name","acc_no"]).agg(
        Shipment=("shipments", "sum"),
        n_s_wc_sum=("n_s_wc", "sum"),
        n_s_za = ("n_s_za","sum"),
        n_s_zb = ("n_s_zb","sum"),
        n_s_zc = ("n_s_zc","sum"),
        kg_0_0_5 = ("kg_0_0_5","sum"),
        kg_0_5_1_0 = ("kg_0_5_1_0","sum"),
        n_wght = ("n_wght","sum"),
        n_rev = ("n_rev","sum"),
        ret_ship_chrg = ("r_s_chrg","sum"),
        r_s_za = ("r_s_za","sum"),
        ret_ship = ("ret_ship","sum"),
        ret_revenue = ("ret_revenue","sum")

    ).reset_index()

    agg_data["ShipmentWithInCity%"] = round((agg_data["n_s_wc_sum"] / agg_data["Shipment"]) * 100,2)
    agg_data["ShipmentzoneA%"] = round((agg_data["n_s_za"] / agg_data["Shipment"]) * 100,2)
    agg_data["ShipmentZoneB%"] = round((agg_data["n_s_zb"] / agg_data["Shipment"]) * 100,2)
    agg_data["ShipmentZoneC%"] = round((agg_data["n_s_zc"] / agg_data["Shipment"]) * 100,2)
    sumWeight = (agg_data["kg_0_0_5"] + agg_data["kg_0_5_1_0"])
    agg_data["0-1kg%"] = round((sumWeight/ agg_data["Shipment"]) * 100,2)
    Total_normalshipA = agg_data["n_s_za"].sum()
    agg_data = agg_data.drop(columns=["n_s_wc_sum"])
    agg_data = agg_data.drop(columns=["n_s_za"])
    agg_data = agg_data.drop(columns=["n_s_zb"])
    agg_data = agg_data.drop(columns=["n_s_zc"])
    agg_data = agg_data.drop(columns=["kg_0_0_5"])
    agg_data = agg_data.drop(columns=["kg_0_5_1_0"])
    TotalRev = agg_data["n_rev"].sum()
    TotalRev_spWise = agg_data.groupby("sp_name")["n_rev"].sum().reset_index()
    agg_data["%DivisonRev(T)"] =round((agg_data["n_rev"]/TotalRev)*100,2)
    agg_data["%DivisonRev(SP)"] =round((agg_data["n_rev"]/TotalRev_spWise["n_rev"])*100,2)
    agg_data["RPS"] = round(agg_data["n_rev"]/agg_data["Shipment"])
    agg_data["RPW"] = round(agg_data["n_rev"]/agg_data["n_wght"])
    agg_data["W/S"] = round(agg_data["n_wght"]/agg_data["Shipment"])
    agg_data["ZONE A% RETURN"] = (agg_data["r_s_za"]/Total_normalshipA)*100
    agg_data["Return Ratio"] = round((agg_data["ret_ship"]/agg_data["Shipment"])*100,2)
    agg_data.drop_duplicates(inplace=True)
    agg_data.fillna(0,inplace=True)
    agg_data.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\SPWISE Presentation_2.csv')

    Logger.log_info("SPWISE Presentation_2 Completed")

SPWISE(Mom_df)

Logger.log_info("SHEET 2 STARTS")

def SHEET2(json_file_path,Mom_df):
    with open(json_file_path, 'r') as file:
        data = json.load(file)
    SP_wise = data['SPWISE']
     
    Domestic_Second_ppt_spwise_IN_total = Mom_df[Mom_df["month"] == SP_wise["CurrMonth"]]
    Domestic_Second_ppt_spwise_IN_totals = Domestic_Second_ppt_spwise_IN_total.groupby("sp_name").agg(
        shipments = ("shipments","sum"),
        n_s_wc = ("n_s_wc","sum"),
        n_s_a = ("n_s_za","sum"),
        kg_0_0_5 = ("kg_0_0_5","sum"),
        kg_0_5_1_0 = ("kg_0_5_1_0","sum")
        
    ).reset_index()
    Domestic_Second_ppt_spwise_IN_totals["Average%_WC"] = round((Domestic_Second_ppt_spwise_IN_totals["n_s_wc"]/Domestic_Second_ppt_spwise_IN_totals["shipments"])*100,2)
    Domestic_Second_ppt_spwise_IN_totals["Average%_ZA"] = round((Domestic_Second_ppt_spwise_IN_totals["n_s_a"]/Domestic_Second_ppt_spwise_IN_totals["shipments"])*100,2)
    Domestic_Second_ppt_spwise_IN_totals["Kg_0_1"] = Domestic_Second_ppt_spwise_IN_totals["kg_0_0_5"] + Domestic_Second_ppt_spwise_IN_totals["kg_0_5_1_0"]
    Domestic_Second_ppt_spwise_IN_totals["Kg_0_1%"] = round((Domestic_Second_ppt_spwise_IN_totals["Kg_0_1"]/Domestic_Second_ppt_spwise_IN_totals["shipments"])*100,2)
    Domestic_Second_ppt_spwise_IN_totals["Average%_WC"] = round((Domestic_Second_ppt_spwise_IN_totals["n_s_wc"]/Domestic_Second_ppt_spwise_IN_totals["shipments"])*100,2)
    Domestic_Second_ppt_spwise_IN_totals.drop("shipments",axis =1,inplace=True)
    Domestic_Second_ppt_spwise_IN_totals.drop("n_s_wc",axis=1,inplace=True)
    Domestic_Second_ppt_spwise_IN_totals.drop("n_s_a",axis=1,inplace=True)
    Domestic_Second_ppt_spwise_IN_totals.drop("kg_0_0_5",axis=1,inplace=True)
    Domestic_Second_ppt_spwise_IN_totals.drop("kg_0_5_1_0",axis=1,inplace=True)
    Domestic_Second_ppt_spwise_IN_totals.drop("Kg_0_1",axis=1,inplace=True)

    Domestic_Second_ppt_spwise_IN_totals.to_csv(r'C:\Users\Pc\Desktop\Domestic\Excel Files Of all PPT 2\SHEET 2 Presentation_2.csv')

    Logger.log_info("SHEET 2 Presentation_2 Completed")

json_file_path = 'input.json'
SHEET2(json_file_path,Mom_df)


     


