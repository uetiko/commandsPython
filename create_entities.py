from pandas import read_csv
from pandas import DataFrame
from tqdm import tqdm

settlementTypeList = list()
zoneList = list()

settlementTypeData = DataFrame(
    read_csv('./resources/data/settlement_type.csv')
)

zoneData = DataFrame(
    read_csv('./resources/data/zone.csv')
)

with tqdm(total=len(settlementTypeData.index)) as bar:
    for index, st in settlementTypeData.iterrows():
        settlementTypeList.append(
            """
            insert into settlement_type(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(st.id), st.settlement_type)
        )
        bar.update(1)

with open('./resources/sql/settlement_type.sql', 'w') as file:
    for row in settlementTypeList:
        file.write(row)

with tqdm(total=len(zoneData.index)) as bar:
    for index, z in zoneData.iterrows():
        zoneList.append(
            """
            insert into zone(id, name)
            values(UUID_TO_BIN('{}'), '{}');
            """.format(str(z.id), z.zone)
        )
        bar.update(1)


with open('./resources/sql/zone.sql', 'w') as file:
    for row in zoneList:
        file.write(row)
