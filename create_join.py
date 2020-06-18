from uuid import uuid4
from sys import argv
from pandas import read_csv
from pandas import DataFrame
from tqdm import tqdm
from alive_progress import alive_bar

"""
create first relation ship between settlemen and settlement type
"""
stateName = argv[1]
csvFile = './resources/{}.csv'.format(stateName)

relationShipList = dict()

mainData = DataFrame(
    read_csv(
        csvFile,
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

settlementData = DataFrame(
    read_csv('./resources/data/settlement_{}.csv'.format(stateName), dtype={
        'settlement_id': str,
        'settlement': str
    })
)
settlementTypeData = DataFrame(
    read_csv(
        './resources/data/settlement_type_{}.csv'.format(stateName),
        dtype={
            'settlement_type_id': str,
            'settlement_type': str
        }
    )
)
postalCodeData = DataFrame(
    read_csv('./resources/data/postalCode_{}.csv'.format(stateName), dtype={
        'postal_code_id': str,
        'postal_code': str
    })
)
postalCodeData = postalCodeData.astype({
    'postal_code_id': str,
    'postal_code': str
})
cityData = DataFrame(
    read_csv('./resources/data/city_{}.csv'.format(stateName), dtype={
        'city_id': str,
        'city': str
    })
)
municipalityData = DataFrame(
    read_csv('./resources/data/municipality_{}.csv'.format(stateName), dtype={
        'municipality_id': str,
        'municipality': str
    })
)
stateData = DataFrame(
    read_csv('./resources/data/state_{}.csv'.format(stateName), dtype={
        'state_id': str,
        'state': str
    })
)
zoneData = DataFrame(
    read_csv('./resources/data/zone_{}.csv'.format(stateName), dtype={
        'zone_id': str,
        'zone': str
    })
)


relationShipList = dict(
    [
        ('id', list()),
        ('municipality_id', list()),
        ('municipality', list()),
        ('postal_code_id', list()),
        ('postal_code', list()),
    ]
)

with alive_bar(len(mainData.index)) as bar:
    for main_index, main_value in mainData.iterrows():
        for m_index, m_row in municipalityData.iterrows():
            if (main_value.D_mnpio == m_row.municipality):
                for p_index, p_value in postalCodeData.iterrows():
                    if (main_value.d_codigo == p_value.postal_code):
                        relationShipList['id'].append(uuid4())
                        relationShipList[
                            'municipality_id'
                        ].append(m_row.municipality_id)
                        relationShipList[
                            'municipality'
                        ].append(m_row.municipality)
                        relationShipList[
                            'postal_code_id'
                        ].append(p_value.postal_code_id)
                        relationShipList[
                            'postal_code'
                        ].append(p_value.postal_code)
                        break
        bar()

(DataFrame(relationShipList)).to_csv(
    './resources/data/relationShip1_{}.csv'.format(stateName), index=False
)

relationShipList = dict(
    [
        ('settlement_id', list()),
        ('settlement', list()),
        ('city_id', list()),
        ('city', list()),
    ]
)

with alive_bar(len(mainData.index)) as bar:
    for m_i, main_row in mainData.iterrows():
        for s_index, settlement_value in settlementData.iterrows():
            if (main_row.d_asenta == settlement_value.settlement):
                for i_ciudad, city_value in cityData.iterrows():
                    if (main_row.d_ciudad == city_value.city):
                        relationShipList[
                            'settlement_id'
                        ].append(settlement_value.settlement_id)
                        relationShipList[
                            'settlement'
                        ].append(settlement_value.settlement)
                        relationShipList[
                            'city_id'
                        ].append(city_value.city_id)
                        relationShipList[
                            'city'
                        ].append(city_value.city)
                        break
        bar()

(DataFrame(relationShipList)).to_csv(
    './resources/data/relationShip2_{}.csv'.format(stateName), index=False
)

relationShipList = dict(
    [
        ('settlement_type_id', list()),
        ('settlement_type', list()),
        ('state_id', list()),
        ('state', list()),
    ]
)

with alive_bar(len(mainData.index)) as bar:
    for m_i, main_row in mainData.iterrows():
        for isettlType, settlementType in settlementTypeData.iterrows():
            if (main_row.d_tipo_asenta == settlementType.settlement_type):
                for istate, state in stateData.iterrows():
                    if (main_row.d_estado == state.state):
                        relationShipList[
                            'settlement_type_id'
                        ].append(settlementType.settlement_type_id)
                        relationShipList[
                            'settlement_type'
                        ].append(settlementType.settlement_type)
                        relationShipList[
                            'state_id'
                        ].append(state.state_id)
                        relationShipList[
                            'state'
                        ].append(state.state)
                        break
        bar()

(DataFrame(relationShipList)).to_csv(
    './resources/data/relationShip3_{}.csv'.format(stateName), index=False
)

relationShipList = dict(
    [
        ('zone_id', list()),
        ('zone', list())
    ]
)

with alive_bar(len(mainData.index)) as bar:
    for m_i, main_row in mainData.iterrows():
        for izone, zone in tqdm(zoneData.iterrows()):
            if (main_row.d_zona == zone.zone):
                relationShipList[
                    'zone_id'
                ].append(zone.zone_id)
                relationShipList[
                    'zone'
                ].append(zone.zone)
                break
        bar()

(DataFrame(relationShipList)).to_csv(
    './resources/data/relationShip4_{}.csv'.format(stateName), index=False
)
