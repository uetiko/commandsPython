from pandas import read_csv
from pandas import DataFrame
from pandas import concat
from os import listdir
from os.path import join
from alive_progress import alive_bar

csvSettlementType = './resources/data/settlement_type.csv'
csvZone = './resources/data/zone.csv'
csvJoinFile = './resources/join/{}'
resourcesPath = './resources/'
resourcesJoinPath = resourcesPath + 'join/'
csvFilesList = list()

zoneDataFrame = DataFrame(
    read_csv('./resources/data/zone.csv', dtype={
        'id': str,
        'zone': str
    }), columns=[
        'id', 'zone'
    ]
)
settlementTypeDataFrame = DataFrame(
    read_csv('./resources/data/settlement_type.csv', dtype={
        'id': str,
        'settlement_type': str
    }), columns=[
        'id', 'settlement_type'
    ]
)

dataTest = DataFrame(
    read_csv('./resources/join/aguascalientes_join.csv')
)

for joinFiles in listdir(resourcesJoinPath):
    if joinFiles.endswith('csv'):
        csvFilesList.append(join(resourcesJoinPath, joinFiles))

for csvFile in csvFilesList:
    dataTest = DataFrame(
        read_csv(csvFile)
    )
    with alive_bar(len(dataTest.index)) as bar:
        for test_index, test_value in dataTest.iterrows():
            for zone_index, zone_value in zoneDataFrame.iterrows():
                if test_value.zone == zone_value.zone:
                    dataTest.loc[test_index, 'zone_id'] = zone_value.id

        dataTest.to_csv(csvFile, index=False)

        for test_index, test_value in dataTest.iterrows():
            for st_index, st_value in settlementTypeDataFrame.iterrows():
                if test_value.settlement_type == st_value.settlement_type:
                    dataTest.loc[test_index, 'settlement_type_id'] = st_value.id

        dataTest.to_csv(csvFile, index=False)
        bar()
