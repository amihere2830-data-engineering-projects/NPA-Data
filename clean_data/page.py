from datetime import datetime



class Page:

    file_path = []
    def __init__(self, pName:str, topic:str) -> None:
        """Initialise the page name and topics
        """
        self.pName = pName
        self.topic = topic

    def allFiles(self,download_data:dict)->list[str]:
        download_files = download_data['raw'][self.pName][self.topic]
        all_download_dates = download_files.keys()

        if len(all_download_dates)==1:
            curr_dw_date = max([datetime.fromisoformat(dt[-10:]) for dt in all_download_dates])\
                .strftime("%Y-%m-%d")
            download_date_current = f'downloaded_at={curr_dw_date}'

            # get paths for urls
            self.file_path.append(f"data/src/raw/{self.pName}/{self.topic}/{download_date_current}/")
            
            return download_files[download_date_current]

        elif len(all_download_dates) > 1:

            dwn_files = []
            for dwn in all_download_dates:

                self.file_path.append(f"data/src/raw/{self.pName}/{self.topic}/{dwn}/")
                
                for fil in download_files[dwn]:
                    dwn_files.append(fil)
                
            return dwn_files

    def currentFiles(self,download_data:dict)->list[str]:
        download_files = download_data['raw'][self.pName][self.topic]
        all_download_dates = download_files.keys()
        curr_dw_date = max([datetime.fromisoformat(dt[-10:]) for dt in all_download_dates])\
            .strftime("%Y-%m-%d")
        download_date = f'downloaded_at={curr_dw_date}'
        return download_files[download_date]