from uuid import uuid4
from sys import argv
from pandas import read_csv
from pandas import DataFrame
from pandas import isnull
from numpy import where
from numpy import nan
import numpy
from tqdm import tqdm

stateName = argv[1]
csvFile = './resources/{}.csv'.format(stateName)

postalCodeList = list()
settlementList = list()
settlementTypeList = list()
municipalityList = list()
cityList = list()
zoneList = list()
stateList = list()

def getDataFrame(csvFile: str) -> DataFrame:
    dataFrame = DataFrame(
        read_csv(csvFile,
            dtype={
                'd_asenta': str,
                'd_tipo_asenta': str,
                'D_mnpio': str,
                'd_estado': str,
                'd_ciudad': str,
                'd_zona': str,
                'd_codigo': str
            }, low_memory=False),
        columns=[
            'd_asenta',
            'd_tipo_asenta',
            'D_mnpio',
            'd_estado',
            'd_ciudad',
            'd_zona',
            'd_codigo'
        ]
    )

    dataFrame = dataFrame.astype({
        'd_asenta': str,
        'd_tipo_asenta': str,
        'D_mnpio': str,
        'd_estado': str,
        'd_ciudad': str,
        'd_zona': str,
        'd_codigo': str
    })

    return dataFrame

def replaceEmpyCitysSaveCSV(dataFrame: DataFrame) -> None:
    dataFrame['d_ciudad'].replace('nan', 'Sin cuidad', inplace=True)
    dataFrame.to_csv(csvFile, index=False)



replaceEmpyCitysSaveCSV(getDataFrame(csvFile))
dataFrame = getDataFrame(csvFile)


result = DataFrame(
    (dataFrame.sort_values(by='d_codigo')).d_codigo.drop_duplicates()
)
result.rename(columns={'d_codigo': 'postal_code'}, inplace=True)
result.loc[:, 'postal_code_id'] = 1
result.loc[:, 'postal_code_id'] = result.groupby(
    'postal_code'
).postal_code.transform(
    lambda uuid: uuid4()
)
result.to_csv('./resources/data/postalCode_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        postalCodeList.append(
            """
            insert into postal_code(id, code)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.postal_code_id), row.postal_code)
        )
        processingBar.update(1)

with open('./resources/data/postalCode_{}.sql'.format(stateName), 'w') as file:
    for row in postalCodeList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='d_tipo_asenta')).d_tipo_asenta.drop_duplicates()
)
result.rename(columns={'d_tipo_asenta': 'settlement_type'}, inplace=True)
result.loc[: , 'settlement_type_id'] = 1
result.loc[: , 'settlement_type_id'] = result.groupby(
    'settlement_type'
).settlement_type_id.transform(lambda uuid: uuid4())
result.to_csv(
    './resources/data/settlement_type_{}.csv'.format(stateName), index=False
)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        settlementTypeList.append(
            """
            insert into settlement_type(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.settlement_type_id), row.settlement_type)
        )
        processingBar.update(1)

with open('./resources/data/settlement_type_{}.sql'.format(stateName), 'w') as file:
    for row in settlementTypeList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='d_asenta')).d_asenta.drop_duplicates()
)
result.rename(columns={'d_asenta': 'settlement'}, inplace=True)
result.loc[: , 'settlement_id'] = 1
result.loc[: , 'settlement_id'] = result.groupby(
    'settlement'
).settlement_id.transform(lambda uuid: uuid4())
result.to_csv('./resources/data/settlement_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        settlementList.append(
            """
            insert into settlement(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.settlement_id), row.settlement)
        )
        processingBar.update(1)

with open('./resources/data/settlement_{}.sql'.format(stateName), 'w') as file:
    for row in settlementList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='D_mnpio')).D_mnpio.drop_duplicates()
)
result.rename(columns={'D_mnpio': 'municipality'}, inplace=True)
result.loc[: , 'municipality_id'] = 1
result.loc[: , 'municipality_id'] = result.groupby(
    'municipality'
).municipality.transform(lambda uuid: uuid4())
result.to_csv('./resources/data/municipality_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        municipalityList.append(
            """
            insert into municipality(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.municipality_id), row.municipality)
        )
        processingBar.update(1)

with open('./resources/data/municipality_{}.sql'.format(stateName), 'w') as file:
    for row in municipalityList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='d_ciudad')).d_ciudad.drop_duplicates()
)
result.rename(columns={'d_ciudad': 'city'}, inplace=True)
result.loc[: , 'city_id'] = 1
result.loc[: , 'city_id'] =  result.groupby(
    'city'
).city.transform(lambda uuid: uuid4())
result.to_csv('./resources/data/city_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        cityList.append(
            """
            insert into city(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.city_id), row.city)
        )
        processingBar.update(1)

with open('./resources/data/city_{}.sql'.format(stateName), 'w') as file:
    for row in cityList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='d_estado')).d_estado.drop_duplicates()
)
result.rename(columns={'d_estado': 'state'}, inplace=True)
result.loc[: , 'state_id'] = 1
result.loc[: , 'state_id'] = result.groupby(
    'state'
).state.transform(lambda uuid: uuid4())
result.to_csv('./resources/data/state_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        stateList.append(
            """
            insert into state(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.state_id), row.state)
        )
        processingBar.update(1)

with open('./resources/data/state_{}.sql'.format(stateName), 'w') as file:
    for row in stateList:
        file.write(row)

result = DataFrame(
    (dataFrame.sort_values(by='d_zona')).d_zona.drop_duplicates()
)
result.rename(columns={'d_zona': 'zone'}, inplace=True)
result.loc[: , 'zone_id'] = 1
result.loc[: , 'zone_id'] = result.groupby(
    'zone'
).zone.transform(lambda uuid: uuid4())
result.to_csv('./resources/data/zone_{}.csv'.format(stateName), index=False)

with tqdm(total=len(result.index)) as processingBar:
    for index, row in result.iterrows():
        zoneList.append(
            """
            insert into zone(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(row.zone_id), row.zone)
        )
        processingBar.update(1)

with open('./resources/data/zone_{}.sql'.format(stateName), 'w') as file:
    for row in zoneList:
        file.write(row)
