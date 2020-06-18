from uuid import uuid4
from pandas import read_csv
from pandas import DataFrame
from pandas import isnull
from pandas import merge
from pandas import concat
from numpy import where
from numpy import nan
from os import listdir
from os.path import isfile, join

mypath = './resources'
csvFilesList = list()
dataList = list()
dataFrameList = list()

def createDataFrame(files: list, column: str)->DataFrame:
    for csvFile in files:
        dataFrame = DataFrame(
            read_csv(csvFile,
                     dtype={
                         column: str
                     }, low_memory=False),
            columns=[
                column,
            ]
        )
        dataFrameList.append(dataFrame)

    return concat(dataFrameList)


for files in listdir(mypath):
    if files.endswith('csv'):
        csvFilesList.append(join(mypath, files))


dataFrame = createDataFrame(csvFilesList, 'd_zona')

dataFrame.drop_duplicates(inplace=True)
dataFrame.rename(columns={'d_zona': 'zone'}, inplace=True)
dataFrame.loc[: , 'id'] = 1
dataFrame.loc[: , 'id'] = dataFrame.zone.transform(
    lambda uuid: uuid4()
)
dataFrame.to_csv('./resources/data/zone.csv', index=False)
