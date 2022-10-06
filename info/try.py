###        NEW            #####
from ast import Delete
import pandas as pd


class TablePaths:

    def __init__(self,data:dict) -> None:
        self.data = data

    # Helper function
    def findPaths(self,topKeys:list[list[str]], dfKeys:list[list[str]])->list[list[str]]:
        """Locates the final paths of the tables"""
        finalPaths = []
        for df_key in dfKeys:
            topkey,lowerkey = df_key
            for dictKey in topKeys:
                if topkey in dictKey:
                    # take the elements in top level 
                    # keys and add the lower key
                    finalPaths.append([*dictKey,lowerkey])
        return finalPaths

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

    # Main function to find all indexes of a file within dictionary data
    def getPaths(self)-> list[list[str]]:
        #identifies dataframe
        foundTable = lambda ky, currntTab:  type(currntTab[ky])==pd.core.frame.DataFrame
        tabs = []
        belowKeys = []
        allDictionarykeys = []
        allTableKeys = []

        # A helper recursive function
        def recurse(currentTable):
            """A recursive function to find paths to DataFrames"""
            for ky in currentTable.keys():
                # subset the dataset must be a dictioary or DataFrame
                if type(currentTable[ky])==dict or type(currentTable[ky])==pd.core.frame.DataFrame:
                    belowKeys.append({ky:list(currentTable[ky].keys())})
                    if foundTable(ky,currentTable):
                        allTableKeys.append(self.findTopIndex(ky,belowKeys))
                        continue
                    else:
                        tabs.append({ky:currentTable})
                        top_indexes = self.findTopIndex(ky,belowKeys)
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
        recurse(self.data)
        print(allDictionarykeys)
        print()
        print(allTableKeys)
        print()
        return self.findPaths(allDictionarykeys,allTableKeys)





table = {'a':{'b':{'c':pd.DataFrame({'bc':[1,2,3]}),
                   'd':pd.DataFrame({'bd':[4,5,6]}),
                   'e':pd.DataFrame({'be':[7,8,9]}),
                   't':pd.DataFrame({'bt':[7,8,9]})},
                   'u':{'s':pd.DataFrame({'us':[3,1,0]})},
              'f':{'h':pd.DataFrame({'fh':[1,4,3]}),
                   'g':pd.DataFrame({'fg':[3,1,0]}),
                   'i':pd.DataFrame({'fi':[3,1,0]}),
                   'j':{'k':pd.DataFrame({'jk':[3,1,0]}),
                        'l':pd.DataFrame({'bc':[1,2,3]})}}
              },
         'm':3
         }


myData = TablePaths(table)

print(myData.getPaths())

# while(True):
#     for i in myData.getPaths()[-1]:
#         table = table[i]
#     print(table)
#     break





###        OLD         ##
from ast import Delete
import pandas as pd


class TablePaths:

    def __init__(self,data:dict) -> None:
        self.data = data

    # Helper function
    def findPaths(self,topKeys:list[list[str]], dfKeys:list[list[str]])->list[list[str]]:
        """Locates the final paths of the tables"""
        finalPaths = []
        for df_key in dfKeys:
            topkey,lowerkey = df_key
            for dictKey in topKeys:
                if topkey in dictKey:
                    tp,lw = dictKey
                    finalPaths.append([tp,lw,lowerkey])
        return finalPaths

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

    # Main function to find all indexes of a file within dictionary data
    def getPaths(self)-> list[list[str]]:
        #identifies dataframe
        foundTable = lambda ky, currntTab:  type(currntTab[ky])==pd.core.frame.DataFrame
        tabs = []
        belowKeys = []
        allDictionarykeys = []
        allTableKeys = []

        # A helper recursive function
        def recurse(currentTable):
            """A recursive function to find paths to DataFrames"""
            for ky in currentTable.keys():
                # subset the dataset must be a dictioary or DataFrame
                if type(currentTable[ky])==dict or type(currentTable[ky])==pd.core.frame.DataFrame:
                    belowKeys.append({ky:list(currentTable[ky].keys())})
                    if foundTable(ky,currentTable):
                        allTableKeys.append(self.findTopIndex(ky,belowKeys))
                        continue
                    else:
                        tabs.append({ky:currentTable})
                        allDictionarykeys.append(self.findTopIndex(ky,belowKeys))
                        recurse(currentTable[ky])
                else:
                    continue
        recurse(self.data)
        print(allDictionarykeys)
        print()
        print(allDictionarykeys)
        print()
        return self.findPaths(allDictionarykeys,allTableKeys)





table = {'a':{'b':{'c':pd.DataFrame({'bc':[1,2,3]}),
                    'u':{'s':pd.DataFrame({'us':[3,1,0]})},
                   'd':pd.DataFrame({'bd':[4,5,6]}),
                   'e':pd.DataFrame({'be':[7,8,9]}),
                   't':pd.DataFrame({'bt':[7,8,9]})},
                   
              'f':{'h':pd.DataFrame({'fh':[1,4,3]}),
                   'g':pd.DataFrame({'fg':[3,1,0]}),
                   'i':pd.DataFrame({'fi':[3,1,0]}),
                   'j':{'k':pd.DataFrame({'jk':[3,1,0]}),
                        'l':pd.DataFrame({'bc':[1,2,3]})}}
              },
         'm':3
         }


myData = TablePaths(table)

print(myData.getPaths())

# while(True):
#     for i in myData.getPaths()[-1]:
#         table = table[i]
#     print(table)
#     break



    # def findkeys(x):
    #     ...

    #     for n, i in enumerate(x):
    #         for j in range(n+1,len(x)):
    #             if list(i.keys())[0] in list(x[j].values())[0]:
    #                 print(f'key [{list(i.keys())[0]}] is a subkey of key [{list(x[j].keys())[0]}]')

# def findkeys():
#     for n, i in enumerate(x):
#         for j in range(n+1,len(x)):
#             if list(i.keys())[0] in list(x[j].values())[0]:
#                 print(f'key [{list(i.keys())[0]}] is a subkey of key [{list(x[j].keys())[0]}]')