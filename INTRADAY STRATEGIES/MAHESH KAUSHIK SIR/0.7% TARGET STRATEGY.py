import pandas as pd
import os
from datetime import datetime, time, timedelta # Make sure timedelta is imported
from dateutil.relativedelta import relativedelta # Still needed for other calcs if used elsewhere, but not for years fraction

# --- Constants and Paths ---
TIME_FORMAT = "%Y-%m-%d %H:%M:%S"
INITIAL_CAPITAL = 20000
YEARS_TO_BACKTEST = 1 # Keep the desired fraction here
DAYS_IN_YEAR_APPROX = 365 # Use an average for calculation
SYMBOLS_FILE = "D:\\STOCK MARKET\\SYMBOLS\\top_10_marketcap.csv"
HISTORICAL_DATA_PATH = "D:\\STOCK MARKET\\HIST_DATA\\INTRADAY DATA\\15 MIN"
OUTPUT_PATH = r"D:\STOCK MARKET\BACKTEST\INTRADAY STRATEGIES\MAHESH KAUSHIK\0.7 TARGET"

# --- Function Definitions ---
def calculate_pp(df_slice, previous_close_value):
    day_high = df_slice['High'].max()
    day_low = df_slice['Low'].min()

    if pd.isna(day_high) or pd.isna(day_low) or pd.isna(previous_close_value):
        return None, None, None, None

    pp = (day_high + day_low + previous_close_value) / 3
    return day_high, day_low, previous_close_value, pp


def backtest(symbol, data_path, initial_capital, years): # 'years' parameter is now fractional
    try:
        df = pd.read_csv(data_path)
        if df.empty:
            print(f"Data file is empty for {symbol}")
            return [], 0, 0, 0

        df['Time'] = pd.to_datetime(df['Time'], format=TIME_FORMAT)
        df.set_index('Time', inplace=True)
        df.sort_index(inplace=True)

    except FileNotFoundError:
        print(f"Data file not found for {symbol}: {data_path}")
        return [], 0, 0, 0
    except Exception as e:
        print(f"Error loading data for {symbol}: {e}")
        return [], 0, 0, 0

    try:
        closes_1515 = df[df.index.time == time(15, 15)]['Close']
        date_to_close_1515 = closes_1515.groupby(closes_1515.index.date).last()
        date_to_prev_close_1515 = date_to_close_1515.shift(1)
        df['PrevDayClose1515'] = df.index.normalize().map(date_to_prev_close_1515)

    except Exception as e:
        print(f"Error pre-calculating previous day's close for {symbol}: {e}")
        return [], 0, 0, 0

    trades = []

    end_date = df.index.max()

    # --- MODIFICATION HERE ---
    # Calculate duration in days using timedelta instead of relativedelta for fractional years
    try:
        duration_in_days = years * DAYS_IN_YEAR_APPROX
        start_date_target = end_date - timedelta(days=duration_in_days)
    except OverflowError:
         # Handle potential overflow if years is excessively large, though unlikely here
         print(f"Error: Calculated duration ({duration_in_days} days) is too large.")
         return [], 0, 0, 0
    # --- END MODIFICATION ---


    available_start_dates = df.index[df.index >= start_date_target]
    if available_start_dates.empty:
         print(f"Not enough data for {symbol} for the requested {years} year(s) period ending {end_date.date()}. Required start date approx {start_date_target.date()}")
         return [], 0, 0, 0
    start_date = available_start_dates.min().normalize()

    df_filtered = df[(df.index >= start_date) & (df.index <= end_date)].copy()

    if df_filtered.empty:
        print(f"No data available for {symbol} in the specified period: {start_date} to {end_date}")
        return [], 0, 0, 0

    unique_days = df_filtered.index.normalize().unique()

    if not unique_days.empty:
        first_day_prev_close_series = df_filtered.iloc[0:1]['PrevDayClose1515']
        if not first_day_prev_close_series.empty and pd.isna(first_day_prev_close_series.iloc[0]):
            if len(unique_days) > 1:
                 print(f"Skipping first day ({unique_days[0].date()}) for {symbol} due to missing previous 15:15 close.")
                 unique_days = unique_days[1:]
            else:
                 print(f"Cannot backtest {symbol}: Missing previous 15:15 close for the only day ({unique_days[0].date()}) in the period.")
                 return [], 0, 0, 0
    else:
         print(f"No unique days found for {symbol} in the specified period.")
         return [], 0, 0, 0


    wins = 0
    losses = 0
    total_profit = 0

    for day in unique_days:
        day_data = df_filtered.loc[df_filtered.index.normalize() == day]

        if len(day_data) < 1:
            continue

        previous_close_1515 = day_data['PrevDayClose1515'].iloc[0]

        if pd.isna(previous_close_1515):
            continue

        time_1045 = datetime.combine(day.date(), time(10, 45))
        if time_1045 not in day_data.index:
            continue

        index_1045_loc = day_data.index.get_loc(time_1045)
        data_until_1045 = day_data.iloc[:index_1045_loc + 1]

        try:
            day_high_pp, day_low_pp, _, pp = calculate_pp(data_until_1045, previous_close_1515)
            if pp is None:
                # print(f"Skipping {day.date()} for {symbol}: Error calculating PP (likely NaN inputs). Prev Close: {previous_close_1515}")
                continue

        except Exception as e:
            print(f"Skipping {day.date()} for {symbol}: Error during PP calculation: {e}. Prev Close: {previous_close_1515}")
            continue

        close_1045 = day_data.loc[time_1045, 'Close']
        day_high_before_1045 = data_until_1045['High'].max()
        day_low_before_1045 = data_until_1045['Low'].min()

        trade_type = None
        if close_1045 > pp:
            trade_type = "LONG"
        elif close_1045 < pp:
            trade_type = "SHORT"

        entry_price = None
        stop_loss = None
        target_price = None
        entry_time = None
        exit_time = None
        exit_price = None
        exit_reason = None
        entry_candle_index = -1

        if trade_type == "LONG":
            for i in range(index_1045_loc + 1, len(day_data)):
                if day_data['High'].iloc[i] > day_high_before_1045:
                    entry_price = day_high_before_1045
                    stop_loss = day_low_before_1045
                    target_price = entry_price * 1.007
                    entry_time = day_data.index[i]
                    entry_candle_index = i
                    break
        elif trade_type == "SHORT":
            for i in range(index_1045_loc + 1, len(day_data)):
                if day_data['Low'].iloc[i] < day_low_before_1045:
                    entry_price = day_low_before_1045
                    stop_loss = day_high_before_1045
                    target_price = entry_price * 0.993
                    entry_time = day_data.index[i]
                    entry_candle_index = i
                    break

        if entry_price and entry_candle_index != -1:
            quantity = initial_capital // entry_price
            if quantity == 0:
                 continue

            exit_price = None
            exit_reason = None
            exit_time = None

            for i in range(entry_candle_index + 1, len(day_data)):
                current_time = day_data.index[i]
                current_high = day_data['High'].iloc[i]
                current_low = day_data['Low'].iloc[i]

                if current_time.time() >= time(15, 15):
                     time_1515 = datetime.combine(day.date(), time(15, 15))
                     if time_1515 in day_data.index:
                         exit_price = day_data.loc[time_1515, 'Close']
                         exit_time = time_1515
                     else:
                         last_candle_before_eod = day_data[day_data.index < datetime.combine(day.date(), time(15, 16))]
                         if not last_candle_before_eod.empty:
                              exit_price = last_candle_before_eod['Close'].iloc[-1]
                              exit_time = last_candle_before_eod.index[-1]
                         else:
                              exit_price = day_data['Close'].iloc[i]
                              exit_time = current_time
                     exit_reason = "End of Day"
                     break

                if trade_type == "LONG":
                    if current_high >= target_price:
                        exit_price = target_price
                        exit_reason = "Target Hit"
                        exit_time = current_time
                        break
                    elif current_low <= stop_loss:
                        exit_price = stop_loss
                        exit_reason = "Stop Loss Hit"
                        exit_time = current_time
                        break
                elif trade_type == "SHORT":
                    if current_low <= target_price:
                        exit_price = target_price
                        exit_reason = "Target Hit"
                        exit_time = current_time
                        break
                    elif current_high >= stop_loss:
                        exit_price = stop_loss
                        exit_reason = "Stop Loss Hit"
                        exit_time = current_time
                        break

            if exit_price is None:
                 exit_price = day_data['Close'].iloc[-1]
                 exit_time = day_data.index[-1]
                 exit_reason = "End of Data"

            profit = (exit_price - entry_price) * quantity if trade_type == "LONG" else (entry_price - exit_price) * quantity
            total_profit += profit

            if profit > 0:
                wins += 1
            else:
                losses += 1

            trades.append({
                'Symbol': symbol,
                'Date': day.strftime('%Y-%m-%d'),
                'Trade Type': trade_type,
                'Entry Time': entry_time.strftime(TIME_FORMAT) if entry_time else None,
                'Entry Price': entry_price,
                'Exit Time': exit_time.strftime(TIME_FORMAT) if exit_time else None,
                'Exit Price': exit_price,
                'Stop Loss': stop_loss,
                'Target Price': target_price,
                'Quantity': quantity,
                'Profit': profit,
                'Exit Reason': exit_reason,
                'Previous Close (15:15)': previous_close_1515,
                'Day High (at PP calc)': day_high_pp,
                'Day Low (at PP calc)': day_low_pp,
                'Pivot Point': pp
            })

    total_trades = wins + losses
    win_rate = wins / total_trades if total_trades > 0 else 0

    win_amounts = [trade['Profit'] for trade in trades if trade['Profit'] > 0]
    loss_amounts = [abs(trade['Profit']) for trade in trades if trade['Profit'] <= 0]

    avg_win = sum(win_amounts) / len(win_amounts) if win_amounts else 0
    avg_loss = sum(loss_amounts) / len(loss_amounts) if loss_amounts else 0

    risk_reward_ratio = avg_win / avg_loss if avg_loss > 0 else 0

    return trades, win_rate, risk_reward_ratio, total_profit


# --- Main Execution ---
if __name__ == "__main__":

    if not os.path.exists(OUTPUT_PATH):
        os.makedirs(OUTPUT_PATH)
        print(f"Created output directory: {OUTPUT_PATH}")

    try:
        symbols_df = pd.read_csv(SYMBOLS_FILE)
    except FileNotFoundError:
        print(f"Symbols file not found: {SYMBOLS_FILE}")
        exit()

    overall_results = []

    print(f"Starting backtest for {YEARS_TO_BACKTEST} year(s)...")

    for symbol in symbols_df['Symbol']:
        print(f"\nProcessing symbol: {symbol}")
        file_path = os.path.join(HISTORICAL_DATA_PATH, f"{symbol}.csv")

        if not os.path.exists(file_path):
             print(f"Data file not found for {symbol}. Skipping.")
             continue

        trades, win_rate, risk_reward, total_profit = backtest(symbol,
                                                                file_path,
                                                                initial_capital=INITIAL_CAPITAL,
                                                                years=YEARS_TO_BACKTEST) # Pass the fractional year value

        if trades is not None:
            print(f"Backtesting results for {symbol}:")
            print(f"  Total Trades: {len(trades)}")
            print(f"  Win Rate: {win_rate:.2%}")
            print(f"  Avg Win / Avg Loss (RRR): {risk_reward:.2f}")
            print(f"  Total Profit: {total_profit:.2f}")

            if trades:
                trades_df = pd.DataFrame(trades)
                trades_df['Entry Time'] = trades_df['Entry Time'].astype(str)
                trades_df['Exit Time'] = trades_df['Exit Time'].astype(str)
                csv_file_path = os.path.join(OUTPUT_PATH, f"{symbol}_trades.csv")
                try:
                    trades_df.to_csv(csv_file_path, index=False)
                    print(f"Trades for {symbol} saved to: {csv_file_path}")
                except Exception as e:
                    print(f"Error saving trades CSV for {symbol}: {e}")
                overall_results.extend(trades)
            else:
                print(f"  No trades executed for {symbol} in the period.")
        else:
             print(f"Backtest skipped or failed for {symbol}.")


    if overall_results:
        print("\n--- Overall Backtesting Summary ---")
        overall_results_df = pd.DataFrame(overall_results)

        # Use the fractional year in the filename
        csv_all_file_path = os.path.join(OUTPUT_PATH, f"ALL_TRADES.csv")
        try:
            overall_results_df['Entry Time'] = overall_results_df['Entry Time'].astype(str)
            overall_results_df['Exit Time'] = overall_results_df['Exit Time'].astype(str)
            overall_results_df.to_csv(csv_all_file_path, index=False)
            print(f"All trades saved to: {csv_all_file_path}")
        except Exception as e:
            print(f"Error saving overall trades CSV: {e}")

        total_trades_overall = len(overall_results_df)
        overall_wins = sum(overall_results_df['Profit'] > 0)
        overall_win_rate = overall_wins / total_trades_overall if total_trades_overall > 0 else 0

        win_amounts = overall_results_df[overall_results_df['Profit'] > 0]['Profit']
        loss_amounts = abs(overall_results_df[overall_results_df['Profit'] <= 0]['Profit'])

        avg_win = win_amounts.mean() if not win_amounts.empty else 0
        avg_loss = loss_amounts.mean() if not loss_amounts.empty else 0
        overall_risk_reward = avg_win / avg_loss if avg_loss > 0 else 0
        overall_profit = overall_results_df['Profit'].sum()

        print(f"  Total Symbols Processed: {len(symbols_df['Symbol'])}")
        print(f"  Total Trades Executed: {total_trades_overall}")
        print(f"  Overall Win Rate: {overall_win_rate:.2%}")
        print(f"  Overall Avg Win / Avg Loss (RRR): {overall_risk_reward:.2f}")
        print(f"  Overall Total Profit: {overall_profit:.2f}")
    else:
        print("\nNo trades were executed across all symbols during the backtest.")
