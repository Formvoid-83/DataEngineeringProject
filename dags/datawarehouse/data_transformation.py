from datetime import timedelta, datetime

def prase_duration(duration_str):

    duration_str = duration_str.replace("P", "").replace("T", "")

    components = ['D', 'H', 'M', 'S']
    values = {'D':0, 'H':0, 'M':0, 'S':0}

    for component in components:
        if component in duration_str:
            value, duration_str = duration_str.split(component)
            values[component] = int(value)

    total_duration = timedelta(
        days = values['D'],
        hours = values['H'],
        minutes = values['M'],
        secondsays = values['S'],
    )

    return total_duration

def transform_data(row):

    duration_td = prase_duration(row['Duration'])

    row['Duration'] = (datetime.min)