from ast import Delete
import pandas as pd

class TablePaths:
    BELOWKEYS_df:list[dict]
    BELOWKEYS_no_df:list[dict]
    PARENT_KEYS:dict

    def __init__(self,data:dict) -> None:
        self.data = data

    # Helper function                                                       
    def findkeys(self,x:list[dict]):
        """Identifies the top level keys all all dictionaries"""
        parent_keys = {}
        for n, i in enumerate(x):
            for j in range(n+1,len(x)):
                if list(i.keys())[0] in list(x[j].values())[0]: # item key in values of other dict? Then a subkey of the dict's key
                    # print(f'key [{list(i.keys())[0]}] is a subkey of key [{list(x[j].keys())[0]}]')
                    # Append all sub keys to the list of parent key
                    try:
                        parent_keys[list(x[j].keys())[0]].insert(0,list(i.keys())[0])
                    except KeyError:
                        parent_keys[list(x[j].keys())[0]]=list(i.keys())
        #Add the parent key at index 0 in the list of their sub keys
        parent_keys = {i:[i,*j] for i, j in parent_keys.items()}
        
        # print(f'parent_keys: {parent_keys}')

        return parent_keys

    # Helper function
    def findPaths(self)->list[list[str]]:
        """Locates the final paths of the tables"""
        final_paths = []
        for key_df in self.BELOWKEYS_df:
            ky,val = list(key_df.items())[0]
            # print(ky)
            for keyys in self.BELOWKEYS_no_df:
                keyys_key = list(keyys.keys())[0]
                if ky in list(keyys.values())[0]:
                    # try:
                    root_keys= [keyys_key,ky]

                    def findAll_keys(pkey,p_keys_val):
                        lenn = 0
                        while(lenn<len(p_keys_val)):
                            p_keys_val = {i:j for i,j in p_keys_val.items() if i!=pkey} #update p_keys_val by removing pkey
                            for parent_keys_key,parent_keys_values in p_keys_val.items():
                                if root_keys[0] in parent_keys_values and parent_keys_key not in root_keys:
                                    root_keys.insert(0,parent_keys_key)
                            lenn+=1

                    findAll_keys(keyys_key,self.PARENT_KEYS)
                    # print(root_keys)
                    final_paths.append(root_keys)
        return final_paths

    # Helper function
    def findTopIndex(self,ky,nw):
        """Search for uper keys of current key in a dictionary"""
        topLevelKeys=[]
        for i in nw:
            if ky in list(i.values())[0]:
                topLevelKeys.append(list(i.keys())[0])
            if ky in list(i.keys())[0]:
                topLevelKeys.append(ky)
        return topLevelKeys
    
    def unique_lists(self, paths):
        """
        Remove duplicate list from the path lists
        """
        join_list_items = lambda x: '='.join(x)
        separate_list_items = lambda x: x.split('=')

        unique_lists = list(set(map(join_list_items, paths)))
        unique_lists = list(map(separate_list_items, unique_lists))
        return unique_lists

    # Main function to find all indexes of a file within dictionary data
    def getPaths(self)-> list[list[str]]:
        #identifies dataframe
        foundTable = lambda ky, currntTab:  type(currntTab[ky])==pd.core.frame.DataFrame
        # tabs = []
        belowKeys_df = []
        belowKeys_no_df = []
        allDictionarykeys = []
        allTableKeys = []

        tab = []
        # A helper recursive function
        def recurse(currentTable):
            """A recursive function to find paths to DataFrames"""
            for ky in currentTable.keys():
                
                # subset the dataset must be a dictioary or DataFrame
                if type(currentTable[ky])==dict or type(currentTable[ky])==pd.core.frame.DataFrame:
                    if foundTable(ky,currentTable):  
                        belowKeys_df.append({ky:list(currentTable[ky].keys())})
                        allTableKeys.append(self.findTopIndex(ky,belowKeys_df))
                        continue
                    else:
                        belowKeys_no_df.insert(0,{ky:list(currentTable[ky].keys())})
                        # tabs.append({ky:currentTable})
                        top_indexes = self.findTopIndex(ky,belowKeys_no_df)
                        #pick the previous top keys and add to the front of new 'top_indexes'
                        try:
                            previous_dict_keys = allDictionarykeys[-1][0:-1]
                            for k in previous_dict_keys:
                                top_indexes.insert(0,k)
                        except IndexError:
                            ...
                        # Add the updated 'top_indexes' to 'allDictionarykeys'
                        lst = []
                        for i in top_indexes:
                            if i not in lst:
                                lst.append(i)
                        allDictionarykeys.append(lst)

                        recurse(currentTable[ky])
                else:
                    continue
        # Run the recursive function to find belowKeys without data frame (belowKeys_no_df),
        # below keys that have data frames (belowKeys_df) and all parent keys (findkeys()) 
        recurse(self.data)

        # Set the values of constants
        self.BELOWKEYS_df = belowKeys_df
        self.BELOWKEYS_no_df = belowKeys_no_df
        self.PARENT_KEYS = self.findkeys(belowKeys_no_df)

        all_paths = self.findPaths()
        if len(all_paths)==0 and len(self.data)==1:
            # Data containts only one data frame
            return self.unique_lists(list(self.data.keys()))
        else:
            return self.unique_lists(all_paths)