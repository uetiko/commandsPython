from uuid import uuid4
from sys import argv
from os import makedirs
from os.path import exists
from datetime import datetime
from pandas import read_csv, DataFrame
from alive_progress import alive_bar
from tqdm import tqdm

joinList = list()
createdAtList = list()
stateName = argv[1]
csvFile = './resources/join/{}_join.csv'.format(stateName)


joinData = DataFrame(read_csv(csvFile))
datetimeAt: datetime = datetime.now()
uuid: uuid4 = uuid4()

createdAtList.append(
    """
    insert into datetime_at(id, created_at, updated_at)
    value(UUID_TO_BIN('{}'), '{}', '{}');
    """.format(
        uuid,
        datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
        datetime.now().strftime('%Y-%m-%d %H:%M:%S')
    )
)

with alive_bar(len(joinData.index)) as bar:
    for index, value in joinData.iterrows():
        if (0 < (datetime.now() - datetimeAt).seconds):
            uuid = uuid4()
            datetimeAt = datetime.now()
            createdAtList.append(
                """
                insert into datetime_at(id, created_at, updated_at)
                value(UUID_TO_BIN('{}'), '{}', '{}');
                """.format(
                    uuid,
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
                    datetime.now().strftime('%Y-%m-%d %H:%M:%S')
                )
            )

        joinList.append(
            """
            insert into catalog_postal_codes(
                id,
                state_id,
                municipality_id,
                settlement_id,
                city_id,
                postal_code_id, zone_id,
                settlement_type_id, datetime_id
            )
            values(
            UUID_TO_BIN('{}'),
            UUID_TO_BIN('{}'),
            UUID_TO_BIN('{}'),
            UUID_TO_BIN('{}'),
            UUID_TO_BIN('{}'),UUID_TO_BIN('{}'),UUID_TO_BIN('{}'),UUID_TO_BIN('{}'),UUID_TO_BIN('{}'));
            """.format(
                value.id,
                value.state_id,
                value.municipality_id,
                value.settlement_id,
                value.city_id,
                value.postal_code_id,
                value.zone_id,
                value.settlement_type_id,
                uuid
            )
        )

directory = './resources/sql/{}'.format(stateName)

if not exists(directory):
    makedirs(directory)

with open(
    './resources/sql/{}/createdAt_{}.sql'.format(
        stateName, stateName
    ), 'a+'
) as createdFile:
    for row in tqdm(createdAtList):
        createdFile.write(row)

with open(
    './resources/sql/{}/postalCodeJoin_{}.sql'.format(
        stateName, stateName
    ), 'a+'
) as joinFile:
    for row in tqdm(joinList):
        joinFile.write(row)
