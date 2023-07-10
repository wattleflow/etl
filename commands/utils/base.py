import os 
import re
import datetime
import urllib

from etl.core.constants import EMPTY
from etl.utils.gis import WattleGis

class WattleUtils:
    def find_file(path, name):
        pass

    def file_exists(file_path):
        return os.path.exists(file_path)

    def get_path(file_path):
        return file_path.replace(os.path.basename(file_path), EMPTY);

    def path_exists(path):
        path_only = WattleUtils.get_path(path)
        return os.path.exists(path_only)
    #     return os.path.isdir(path)

    def find_file(path, filename):
        for root, dirs, files in os.walk(path):
            for file in files:
                if file == filename:
                    return os.path.join(root, file)
        return None

    def get_all_files(directory):
        file_list = []
        for root, dirs, files in os.walk(directory):
            for file in files:
                file_list.append(os.path.join(root, file))
        return file_list
      
    def make_dir(path):
        path_only = path.replace(os.path.basename(path), EMPTY);
        if os.path.exists(path): return True
        os.mkdir(path_only)
        return WattleUtils.path_exists(path)

    def change_extension(file_path, ext):
        extension = os.path.splitext( os.path.basename(file_path) )[1]
        return file_path.replace(extension, ext)

    def extract_patterns(text, patterns, flags=re.M):
        pattern_list = list()

        if isinstance(patterns, list):
            for n, token in enumerate(patterns):
                pattern_list.append( [n, re.compile(token, flags)] )
        elif isinstance(patterns, dict):
            for order, token in patterns.items():
                pattern_list.append( [order, re.compile(token, flags)]) 
        else:
            raise Exception("Utils.extract_patterns: patterns is unkown type")  

        result = list()
        for pattern in pattern_list:
            i = pattern[0]
            m = pattern[1].findall(text)
            if m:
                result.append((i, m))
        return result

    def url_quote(url):
        return urllib.parse.quote(url); 

    def url_encode(url, params): 
        return urllib.parse.urlencode(url, params=params)

    def update_gis(df, point, dropcol=False, longitude='longitude', latitude='latitude',):
        if point in df.columns:
            df[longitude] = df[point].apply(lambda x: WattleGis(x).longitude())
            df[latitude] = df[point].apply(lambda x: WattleGis(x).latitude())
            if dropcol:
                df = df.drop([point], axis=1)
        return df

    def unique(self, df, column):
        return sorted(df[column].apply(lambda x: x.strip() if isinstance(x, str) else x).dropna().unique())

    def show_distinct(self, df, col='', mx=10):
        distinct, records = [], df.shape[0]
        log = []
        if col in df.columns:
            distinct = sorted(df[col].unique())
            uniques = len(distinct)
            nans = int(df[col].isna().sum())
            itms = [ str(item) for item in distinct ]
            show = mx if len(itms) > mx else len(itms)
            log.append("\nField \"{}\" has ({}:{}) unique values and ({}) NaN entries found.\n>>> {}".format(
                col,
                show,
                uniques,
                nans,
                ", ".join(itms[:show]))
            )
        return "{}".format(str(log))

    def get_field_unique_values(df, key, items=5, spaces=18):
        unq = sorted(df[key].astype(str).unique())
        if len(unq) > items:
            return "{} : {} of ({}) values.".format(key.ljust(spaces), unq[:items], len(unq))
        else:
            return "{} : [{}] of ({}) values.".format(key.ljust(spaces), ', '.join(unq), len(unq))

    def get_dataframe_details(df, ticks, inrows=None):
        iserror = 'error' in df.columns
        if iserror:
            return "Error: {}\n{}".format(df.iloc[0][0], df.iloc[0][1])
        log = []
        rows, flds, msec = df.shape[0], len(df.columns), ticks * 1000
        if inrows is None: 
            return f"({rows}) records in ({flds}) field(s)";

        names = '\",\n\"'.join(df.columns) if inrows else  '\", \"'.join(df.columns)
        if inrows:
            return f"({rows}) records in ({flds}) field(s) | {msec:.3f} ms\n(\"{names}\")"
        else:
            return f"({rows}) records in ({flds}) field(s) | {msec:.3f} ms\n[\"{names}\"]"