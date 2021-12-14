import numpy as np
import pandas as pd
import urllib
import json

class Logger:
    def _log_err(self,msg):
        self._log(f'Error: {msg}')

    def _log(self,msg):
        if hasattr(self,"datasets"):
            name = "Explorer"
        else:
            name = self.resource_id if not hasattr(self,"raw") or "meta" not in self.raw  or "result" not in self.raw["meta"] else self.raw["meta"]["result"]["name"]
        print(f'[{name}] {msg}')

class Dataset(Logger):
    API_ROUTES = {
        "data": "https://data.gov.sg/api/action/datastore_search",
        "meta": "https://data.gov.sg/api/action/resource_show"
    }
    API_HEADERS = {"User-Agent": "Mozilla/5.0 (Windows NT 6.1) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/41.0.2228.0 Safari/537.3"}
    
    def __init__(self,i):

        self.resource_id = i
        self.fetch()
        self.parse()
        if hasattr(self,"parsed") and "data" in self.parsed:
            self.dataframe = self._to_DataFrame(self.parsed['data'])
            self._log("Loaded")
        else: 
            self._log("Unable to load data")

        
    def fetch_route(self,route="data"):
        self._log(f'Fetching {"metadata" if route == "meta" else route} via API')
        try:
            request_url = f'{self.API_ROUTES[route]}?{"resource_id" if route == "data" else "id"}={self.resource_id}&limit=10000'
            if route == "data":
                datecols = self._get_datetime_cols()
                if len(datecols):
                    request_url += f'&sort={datecols[0]["name"]}%20desc'
            req = urllib.request.Request(
                url=request_url, 
                headers=self.API_HEADERS
            )
            response = urllib.request.urlopen(req).read() 
            return json.loads(response)
        except urllib.error.HTTPError as e:
            self._log_err(f'({e.code}) {e.msg}')
        
    def fetch(self):
        try:
            self.raw = {}
            self.raw["meta"] = self.fetch_route('meta')
            self.raw["data"] = self.fetch_route('data')
        except Exception as e:
            self._log_err(e)
            
    def parse(self):
        try:
            self.parsed = {}
            for key in self.raw:
                self._log(f'Parsing {"metadata" if key == "meta" else key}')
                subset = self.raw[key]
                if subset["success"]:
                    self.parsed[key] = subset["result"] if key == "meta" else subset["result"]["records"]
        except Exception as e:
            self._log_err(e)
            
    def _to_DataFrame(self,data):
        self._log("Converting data to Pandas DataFrame")
        df = pd.DataFrame.from_dict(data)
        df = df.set_index("_id")
        datecols = self._get_datetime_cols()
        datecolnames = [col["name"] for col in datecols]
        for col in df:
            null_values = self._get_col_nulls(col)
            for null in null_values:
                df[col] = df[col].replace(null,np.NaN)
            if col in datecolnames:
                col_meta = list(filter(lambda c: c["name"] == col,datecols))[0]
                date_format = col_meta["format"].replace("YYYY","%Y").replace("MM","%m").replace("[Q]Q","%m")
                df[col] = df[col].apply(lambda x: x.replace("Q1","03"))
                df[col] = df[col].apply(lambda x: x.replace("Q2","06"))
                df[col] = df[col].apply(lambda x: x.replace("Q3","09"))
                df[col] = df[col].apply(lambda x: x.replace("Q4","12"))
                df[col] = pd.to_datetime(df[col],format=date_format)
            else:
                try:
                    df[col] = pd.to_numeric(df[col])
                except Exception as e:
                    self._log(f'Column "{col}" does not appear to be numeric. Pandas says: "{str(e)}"')
                    pass
        return df

    def _get_meta(self):
        return self.raw["meta"]["result"] if not hasattr(self,"parsed") else self.parsed["meta"]

    def _get_col_nulls(self,col):
        meta = self._get_meta()
        col_field_meta = list(filter(lambda field: field["name"] == col,meta["fields"]))
        if len(col_field_meta):
            return [char for char in col_field_meta[0]["null_values"].keys() if char != "count"]
        return []

    def _get_datetime_cols(self):
        try:
            meta = self._get_meta()
            return [field for field in meta["fields"] if field["type"] == "datetime"]
        except Exception as e:
            self._log_err(e)

class Explorer(Logger):
    def __init__(self,datasets):
        self.datasets = datasets
        for dataset in self.datasets:
            print("\n")
            self._log(f'Analyzing "{dataset.parsed["meta"]["name"]}"')
            print("-"*50)
            print("Describe")
            print("-"*10)
            print(dataset.dataframe.describe(datetime_is_numeric=True))
            print("\n")
            print("Head")
            print("-"*10)
            print(dataset.dataframe.head())
            dataset.dataframe.hist(bins=10)
            datecols = dataset._get_datetime_cols()
            dataset.dataframe.plot(x=datecols[0]["name"])

            
            