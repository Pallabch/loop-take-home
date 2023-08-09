import pandas as pd
from datetime import datetime, timedelta
import pytz
from tqdm import tqdm
import os
from dotenv import load_dotenv

load_dotenv()

class compute:
    def __init__(self):
        self.store_status = pd.read_csv(os.getenv("FILE_STORE_STATUS"))
        self.menu_hours = pd.read_csv(os.getenv("FILE_MENU_HOURS"))
        self.time_zones = pd.read_csv(os.getenv("FILE_TIME_ZONES"))
        self.time_format1 = "%Y-%m-%d %H:%M:%S %Z"
        self.time_format2 = "%Y-%m-%d %H:%M:%S.%f %Z"

    def customStoreStatus(self, target_store_id):
        try:
            selected_timezone = self.time_zones[self.time_zones.store_id == target_store_id].iloc[0, 1]
            if selected_timezone not in pytz.all_timezones:
                selected_timezone = "America/Chicago"
        except:
            selected_timezone = "America/Chicago"

        status_list = []
        for store_row in self.store_status[self.store_status.store_id == target_store_id].itertuples(index=False):
            try:
                timestamp_temp = datetime.strptime(store_row.timestamp_utc, self.time_format1)
            except:
                timestamp_temp = datetime.strptime(store_row.timestamp_utc, self.time_format2)
            utc_timezone = pytz.timezone("UTC")
            timestamp_temp_with_utc = utc_timezone.localize(timestamp_temp)
            new_selected_timezone = pytz.timezone(selected_timezone)
            converted_timestamp = timestamp_temp_with_utc.astimezone(new_selected_timezone)
            status_list.append((converted_timestamp, store_row.status))
        sorted_status_list = sorted(status_list, key=lambda x: x[0])
        return sorted_status_list

    def getDataForDate(self, target_date, sorted_status_list):
        data_for_date = []
        for item in sorted_status_list:
            if (
                item[0].day == target_date.day
                and item[0].year == target_date.year
                and item[0].month == target_date.month
            ):
                data_for_date.append(item)
        return data_for_date

    def calculateUptimeDowntime(self, data_for_day, target_store_id):
        if data_for_day is None or len(data_for_day) == 0:
            return 0, 0
        day_of_week = data_for_day[0][0].weekday()
        day = data_for_day[0][0].day
        month = data_for_day[0][0].month
        year = data_for_day[0][0].year
        timezone_info = data_for_day[0][0].tzinfo

        open_intervals = []
        if len(self.menu_hours[(self.menu_hours.store_id == target_store_id) & (self.menu_hours.day == day_of_week)].value_counts()) == 0:
            start_time = datetime.strptime("00:00:00", "%H:%M:%S")
            start_time = start_time.replace(year=year, month=month, day=day)
            start_time = timezone_info.localize(start_time)
            end_time = datetime.strptime("23:59:59", "%H:%M:%S")
            end_time = end_time.replace(year=year, month=month, day=day)
            end_time = timezone_info.localize(end_time)
            open_intervals.append((start_time, end_time))
        else:
            for menu_row in self.menu_hours[(self.menu_hours.store_id == target_store_id) & (self.menu_hours.day == day_of_week)].itertuples(index=False):
                start_time = datetime.strptime(menu_row.start_time_local, "%H:%M:%S")
                start_time = start_time.replace(year=year, month=month, day=day)
                start_time = timezone_info.localize(start_time)
                end_time = datetime.strptime(menu_row.end_time_local, "%H:%M:%S")
                end_time = end_time.replace(year=year, month=month, day=day)
                end_time = timezone_info.localize(end_time)
                open_intervals.append((start_time, end_time))

        interval_data = {}
        for entry in data_for_day:
            for interval in open_intervals:
                if interval[0] < entry[0] < interval[1]:
                    interval_data.setdefault(interval, []).append(entry)

        uptime = 0
        downtime = 0
        for interval, interval_entries in interval_data.items():
            time_from_interval_start = interval_entries[0][0] - interval[0]
            time_from_interval_start = time_from_interval_start.total_seconds() / 60
            if interval_entries[0][1] == "active":
                uptime += time_from_interval_start
            else:
                downtime += time_from_interval_start

            for i in range(len(interval_entries) - 1):
                time_between_entries = interval_entries[i + 1][0] - interval_entries[i][0]
                time_between_entries = time_between_entries.total_seconds() / 60
                if interval_entries[i][1] == "active":
                    uptime += time_between_entries
                else:
                    downtime += time_between_entries
            time_until_interval_end = interval[1] - interval_entries[-1][0]
            time_until_interval_end = time_until_interval_end.total_seconds() / 60
            if interval_entries[-1][1] == "active":
                uptime += time_until_interval_end
            else:
                downtime += time_until_interval_end

        return uptime, downtime

    def calculateWeeklyStatus(self, data, target_store_id, start_date):
        total_active = 0
        total_inactive = 0
        store_status_list = self.customStoreStatus(target_store_id)
        day_counter = 6

        while not data and day_counter > 0:
            start_date = start_date - timedelta(days=1)
            data = self.getDataForDate(start_date, store_status_list)
            day_counter -= 1

        if not data:
            return 0, 0, 0, 0

        active_day, inactive_day = self.calculateUptimeDowntime(data, target_store_id)
        total_active += active_day
        total_inactive += inactive_day
        weekly_active_day = active_day
        weekly_inactive_day = inactive_day

        for i in range(6):
            previous_date = data[0][0] - timedelta(days=i)
            previous_day_data = self.getDataForDate(previous_date, store_status_list)
            a, i = self.calculateUptimeDowntime(previous_day_data, target_store_id)
            total_active += a
            total_inactive += i

        return total_active, total_inactive, weekly_active_day, weekly_inactive_day

    def getDataInHour(self, target_datetime, status_list):
        last_hour_datetime = target_datetime - timedelta(hours=1)
        
        for status_entry in status_list:
            if (
                status_entry[0].year == target_datetime.year
                and status_entry[0].month == target_datetime.month
                and status_entry[0].day == target_datetime.day
            ):
                if last_hour_datetime.hour <= status_entry[0].hour <= target_datetime.hour:
                    return status_entry

    def calculateLastHourStatus(self, data_entry):
        if not data_entry:
            return 0, 0
        
        if data_entry[1] == "active":
            return 60, 0
        else:
            return 0, 60

    def calculateWeeklyDayStatusForAllStores(self, status_data, current_date=datetime(2023, 1, 25, 19, 0, 0)):
        store_info = {}
        
        for store_id in tqdm(status_data.store_id.unique()):
            store_status_list = self.customStoreStatus(store_id)
            status_at_current_date = self.getDataForDate(current_date, store_status_list)
            data_hour_at_current_date = self.getDataInHour(current_date, store_status_list)
            
            active_hour, inactive_hour = self.calculateLastHourStatus(data_hour_at_current_date)
            active_week, inactive_week, active_day, inactive_day = self.calculateWeeklyStatus(status_at_current_date, store_id, current_date)
            
            store_info[store_id] = (active_week, inactive_week, active_day, inactive_day, active_hour, inactive_hour)
        
        return store_info


    def saveDataToCSV(self, data_dictionary, file_path):
        df = pd.DataFrame.from_dict(
            data_dictionary,
            orient="index",
            columns=[
                "uptime_last_week",
                "downtime_last_week",
                "uptime_last_day",
                "downtime_last_day",
                "uptime_last_hour",
                "downtime_last_hour",
            ],
        )
        df.index.name = "store_id"
        df.reset_index(inplace=True)
        df.to_csv(file_path, index=False)
