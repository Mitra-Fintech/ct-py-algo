from pmClient.WebSocketClient import WebSocketClient
import logging
import csv
import datetime from datetime




webSocketClient = WebSocketClient("eyJ0eXAiOiJKV1QiLCJhbGciOiJIUzI1NiJ9.eyJhdWQiOiJtZXJjaGFudCIsImlzcyI6InBheXRtbW9uZXkiLCJpZCI6NzI2NDQ1LCJleHAiOjE3MDMxODMzOTl9.Wi0UOHrwOVHT0OLeG8pDc4oPt4PY8TezqbgyEmzFUOU")



customerPreferences = []
preference = {
    "actionType": 'ADD',  # 'ADD', 'REMOVE'
    "modeType": 'LTP',  # 'LTP', 'FULL', 'QUOTE'
    "scripType": 'INDEX',  # 'ETF', 'FUTURE', 'INDEX', 'OPTION', 'EQUITY'
    "exchangeType": 'NSE',  # 'BSE', 'NSE'
    "scripId": '13'
}

customerPreferences.append(preference)


myPosition = 0
myPositionFor = ""


def calculate_ce_pe(last_price: float, round_interval: int = 50, ce_offset: int = 150, pe_offset: int = -300) -> tuple:
    """
    Calculate the CE (Call Option) and PE (Put Option) strike prices based on the last price.

    Args:
        last_price (float): The original last price.
        round_interval (int): The interval to round the last price to. Defaults to 50.
        ce_offset (int): The offset added to the rounded up price to calculate the CE strike price. Defaults to 150.
        pe_offset (int): The offset added to the CE strike price to calculate the PE strike price. Defaults to -300.

    Returns:
        tuple: The CE strike price and PE strike price as a tuple.
    """
    if last_price is None:
        # Handle the case when last_price is not available
        logging.warning("Last price is not available.")
        return None, None
    
    # Log the original last_price
    logging.info(f"Original last_price: {last_price}")

    # Round last_price to the nearest multiple of round_interval
    last_price = round(last_price / round_interval) * round_interval
    logging.info(f"Rounded last_price: {last_price}")

    # Calculate rounded_down based on last_price
    rounded_down = round(last_price / round_interval) * round_interval

    # Calculate rounded_up by adding round_interval to rounded_down
    rounded_up = rounded_down + round_interval
    logging.info(f"Rounded Down: {rounded_down}, Rounded Up: {rounded_up}")

    # Calculate CE and PE strike prices
    ce_strike = rounded_up + ce_offset
    pe_strike = ce_strike + pe_offset
    logging.info(f"CE Strike: {ce_strike}, PE Strike: {pe_strike}")

    return ce_strike, pe_strike


def find_id_by_key(file_path, search_key):
    # print(f"Searching for '{search_key}' in '{file_path}'... at line 56")
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        header = next(reader)  # Skip the header row if it exists
        for row in reader:
            if len(row) <= 14: 
                entry = {
                    "ID": row[0],
                    "Symbol": row[1],
                    "Description": row[2],
                    "Lot Size": row[3],
                    # "Quantity": row[4],
                    # "Instrument": row[5],
                    # "Type": row[6],
                    # "Exchange": row[7],
                    # "Price": row[8],
                    # "Strike Price": row[9],
                    # "Timestamp": row[10],
                    # "Expiry Date": row[11],
                    # "Open Interest": row[12]
                }
                # if search_key in entry.values():
                #     return entry["ID"]
                if search_key.lower() in entry["Description"].lower():
                    return entry["ID"]
            else:
                print(f"Skipping row: {row} - Insufficient values")
    return None  # Return None if the key is not found

def find_symbol_by_id(file_path, security_id):
    with open(file_path, 'r') as csv_file:
        reader = csv.reader(csv_file, delimiter=',')
        header = next(reader)  # Skip the header row if it exists
        for row in reader:
            if len(row) >= 1:
                if int(row[0]) == security_id:
                    return row[1]  # Assuming the symbol name is in the third column (index 2)
    return None

def manage_position(position):
    myPosition = position



def on_open():
    # send preferences via websocket once connection is open
    webSocketClient.subscribe(customerPreferences)


def on_close(code, reason):
    # this event gets triggered when connection is closed
    print(code, reason)


def on_error(error_message):
    # this event gets triggered when error occurs
    print(error_message)

def start_time(): # Starting time at 9:15
    current_time1 = datetime.datetime.now().time()
    start_time = datetime.time(9, 15)
    return current_time1 => start_time

def stop_time():   #Stop time at 3:24
    current_time2 = datetime.datetime.now().time()
    stop_time = datetime.time(3, 24)
    return current_time2 =< stop_time    
 


def on_message(arr):
    # this event gets triggered when response is received
    if arr:
        print(arr)

        # get the last price from the response and calculate CE and PE
        # response is [{'last_price': 139.3, 'last_trade_time': 1703147740, 'security_id': 65615, 'tradable': 1, 'mode': 61, 'change_absolute': -19.75, 'change_percent': -12.42}, {'last_price': 218.1, 'last_trade_time': 1703147740, 'security_id': 65622, 'tradable': 1, 'mode': 61, 'change_absolute': -31.35, 'change_percent': -12.57}, {'last_price': 188.55, 'last_trade_time': 1703147740, 'security_id': 65620, 'tradable': 1, 'mode': 61, 'change_absolute': -26.7, 'change_percent': -12.4}, {'last_price': 102.1, 'last_trade_time': 1703147740, 'security_id': 65603, 'tradable': 1, 'mode': 61, 'change_absolute': -18.7, 'change_percent': -15.48}]
        # last_price = arr[0]['last_price']

        # get last price from the arr where security_id is 13 and has multiple entries
        last_price = None
        for entry in arr:
            if entry.get('security_id') == 13:
                    last_price = entry.get('last_price')

            if last_price:
                print(f"Last Price: {last_price}")
            # segregate all the arr and keep it in seperate files name as security id.txt, if file is not available then create one in current directory
            import os

            # get the current working directory
            current_directory = os.getcwd()
            # create a new directory with the name of the security id for each security id in the arr
            for entry in arr:
                security_id = entry.get('security_id')
                if security_id:
                    symbol_name = find_symbol_by_id("securities-list.csv", security_id)
                    directory_name = str(symbol_name)
                    directory_path = os.path.join(current_directory, directory_name)
                    if not os.path.exists(directory_path):
                        os.makedirs(directory_path)

            # create a new file in the directory with the name of the security id
            for entry in arr:
                security_id = entry.get('security_id')
                if security_id:
                    symbol_name = find_symbol_by_id("securities-list.csv", security_id)
                    file_name = symbol_name + '.txt'
                    file_path = os.path.join(current_directory, symbol_name, file_name)
                    with open(file_path, 'a') as file:
                        file.write(str(entry) + '\n')




        if last_price is not None:

            ce_strike_price, pe_strike_price = calculate_ce_pe(last_price)

            # print the calculated CE and PE
            print(f"CE Strike Price: {ce_strike_price}")
            print(f"PE Strike Price: {pe_strike_price}")

            # Example usage
            file_path = 'securities-list.csv'  # Replace with your actual file path
            # search_key = 'NIFTY 28 DEC ' + str(pe_strike_price) + ' PUT'  # Replace with the key you're searching for
            
            # print(f"Searching for '{search_key}' in '{file_path}'...")

            # found_id = find_id_by_key(file_path, search_key)

            # if found_id is not None:
            #     print(f"The first ID for the key '{search_key}' is: {found_id}")
            # else:
            #     print(f"The key '{search_key}' was not found in the CSV file.")

            flagCounter = 0
            hopper = 50
            
            # CE 
            intial_ce_strike = ce_strike_price
            current_ce_strike = intial_ce_strike

            # PE
            intial_pe_strike = pe_strike_price
            current_pe_strike = intial_pe_strike


            while(flagCounter == 0):
                # check if the intial ce_strike and current ce_strike has a difference of 1000
                # if (intial_ce_strike - current_ce_strike) >= 1000:
                #     flagCounter = 1
                #     break
                # else:
                current_ce_strike = current_ce_strike + hopper

                print(f"CE Strike Price: {current_ce_strike}")

                search_key = 'NIFTY 28 DEC ' + str(current_ce_strike) + ' CALL'

                # print(f"Searching for '{search_key}' in '{file_path}'...")

                found_id = find_id_by_key(file_path, search_key)

                if found_id is not None:
                    print(f"The first ID for the key '{search_key}' is: {found_id}")
                    preference = {
                        "actionType": 'ADD',  # 'ADD', 'REMOVE'
                        "modeType": 'LTP',  # 'LTP', 'FULL', 'QUOTE'
                        "scripType": 'OPTION',  # 'ETF', 'FUTURE', 'INDEX', 'OPTION', 'EQUITY'
                        "exchangeType": 'NSE',  # 'BSE', 'NSE'
                        "scripId": found_id
                    }
                else:
                    print(f"The key '{search_key}' was not found in the CSV file.")
                customerPreferences.append(preference)

                # do all the same for PE
                # if (intial_pe_strike - current_pe_strike) >= 1000:
                #     flagCounter = 1
                #     break
                # else:
                current_pe_strike = current_pe_strike + hopper

                print(f"PE Strike Price: {current_pe_strike}")

                search_key = 'NIFTY 28 DEC ' + str(current_pe_strike) + ' PUT'

                # print(f"Searching for '{search_key}' in '{file_path}'...")

                found_id = find_id_by_key(file_path, search_key)

                if found_id is not None:
                    print(f"The first ID for the key '{search_key}' is: {found_id}")
                    preference = {
                        "actionType": 'ADD',  # 'ADD', 'REMOVE'
                        "modeType": 'LTP',  # 'LTP', 'FULL', 'QUOTE'
                        "scripType": 'OPTION',  # 'ETF', 'FUTURE', 'INDEX', 'OPTION', 'EQUITY'
                        "exchangeType": 'NSE',  # 'BSE', 'NSE'
                        "scripId": found_id
                    }
                else:
                    print(f"The key '{search_key}' was not found in the CSV file.")

                
                # print((intial_ce_strike - current_ce_strike))

                customerPreferences.append(preference)

                webSocketClient.subscribe(customerPreferences)
                

                if (current_ce_strike - intial_ce_strike) >= 1000 or (current_pe_strike - intial_pe_strike) >= 1000:
                    flagCounter = 1
                    break




    else:
        print("Received an empty response.")


    





webSocketClient.set_on_open_listener(on_open)
webSocketClient.set_on_close_listener(on_close)
webSocketClient.set_on_error_listener(on_error)
webSocketClient.set_on_message_listener(on_message)

"""
set below reconnect config if reconnect feature is desired
Set first param as true and second param, the no. of times retry to connect to server shall be made  
"""
webSocketClient.set_reconnect_config(True, 5)

# this method is called to create a websocket connection with broadcast server
webSocketClient.connect()
start_time()
stop_time()

